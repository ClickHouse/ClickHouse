#include <Interpreters/JIT/CompileRegexp.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <limits>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <Common/Exception.h>
#include <Common/RegexpJIT/RegexpProgram.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <base/types.h>

#include <Interpreters/JIT/CHJIT.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
}

using namespace RegexpJIT;

namespace
{
    std::mutex regexp_jit_mutex;
    std::shared_ptr<CHJIT> regexp_jit_instance;

    /// Counts how many times each (pattern, flags) key has been seen, so a pattern is compiled only after
    /// `min_count_to_compile` uses (mirrors `compile_expressions`). It is bounded: cleared wholesale once
    /// it grows past `MAX_SEEN_COUNTER_ENTRIES`, and also reset by `resetRegexpJITInstance` (i.e. by
    /// `SYSTEM DROP COMPILED EXPRESSION CACHE`), so the default-enabled path cannot accumulate unbounded
    /// process-wide state for workloads with many unique patterns.
    std::mutex regexp_seen_counter_mutex;
    std::unordered_map<UInt128, UInt64, UInt128Hash> regexp_seen_counter;
    constexpr size_t MAX_SEEN_COUNTER_ENTRIES = 10000;

    /// Members of a set listed individually are only used when the set (or its complement) is small.
    constexpr size_t SIMD_MEMBERS_MAX = 8;
    constexpr uint32_t UNBOUNDED = std::numeric_limits<uint32_t>::max();
}

static std::shared_ptr<CHJIT> getRegexpJITInstancePtr()
{
    std::lock_guard lock(regexp_jit_mutex);
    if (!regexp_jit_instance)
        regexp_jit_instance = std::make_shared<CHJIT>();
    return regexp_jit_instance;
}

void resetRegexpJITInstance()
{
    {
        std::lock_guard lock(regexp_jit_mutex);
        regexp_jit_instance.reset();
    }
    {
        std::lock_guard lock(regexp_seen_counter_mutex);
        regexp_seen_counter.clear();
    }
}

namespace
{

/// Holds a compiled regexp matcher module and keeps its `CHJIT` instance alive (see `CompiledFunctionHolder`).
class CompiledRegexpHolder : public CompiledExpressionCacheEntry
{
public:
    CompiledRegexpHolder(CHJIT::CompiledModule module_, std::shared_ptr<CHJIT> jit_, JITRegexpMatcherFunc func_, int num_captures_)
        : CompiledExpressionCacheEntry(module_.size)
        , module(module_)
        , jit(std::move(jit_))
        , func(func_)
        , num_captures(num_captures_)
    {
    }

    ~CompiledRegexpHolder() override
    {
        try
        {
            jit->deleteCompiledModule(module);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    CHJIT::CompiledModule module;
    std::shared_ptr<CHJIT> jit;
    JITRegexpMatcherFunc func = nullptr;
    int num_captures = 1;
};

/// True if `op` can never advance the cursor, so it is transparent when locating the first
/// cursor-advancing operation for the leading-run skip (see `RegexpEmitter::emit`): a zero-width
/// capture marker, or an optional whose body is itself entirely zero-width (e.g. `()?`, `(?:)?`,
/// `((?:)?)?`). A `CharQuant` is never treated as zero-width, even with `min == 0`, since it can
/// still consume input; anchors are conservatively treated as opaque here (the shortcut is simply
/// not applied through them, which stays correct).
bool isZeroWidthOp(const Op & op)
{
    switch (op.kind)
    {
        case OpKind::CaptureStart:
        case OpKind::CaptureEnd:
            return true;
        case OpKind::Optional:
            for (const Op & inner : op.body)
                if (!isZeroWidthOp(inner))
                    return false;
            return true;
        default:
            return false;
    }
}

/// Emits LLVM IR for the per-string matcher described by a `RegexpProgram`.
///
/// The matcher is a depth-first, greedy, backtracking search over the program's operations,
/// expressed entirely with basic blocks: each operation either advances the cursor and continues
/// with the rest of the program (in continuation-passing style) or branches to a failure label.
/// Variable-length operations (`CharQuant`, `Optional`) introduce backtracking points - a greedy
/// "give-back" loop for quantifiers and a body/skip fork for optionals - which keeps the matcher
/// equivalent to RE2 on the supported subset without an explicit backtracking stack.
class RegexpEmitter
{
public:
    RegexpEmitter(llvm::Module & module_, const RegexpProgram & program_)
        : module(module_)
        , context(module_.getContext())
        , b(context)
        , program(program_)
    {
    }

    void emit()
    {
        auto * i8_ptr = bytePtrTy();
        auto * i8_ptr_ptr = i8_ptr->getPointerTo();
        auto * func_type = llvm::FunctionType::get(b.getInt8Ty(), {i8_ptr, i8_ptr, i8_ptr, i8_ptr_ptr, i8_ptr_ptr}, false);
        function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "regexp_match", module);

        begin_arg = function->getArg(0);
        end_arg = function->getArg(1);
        search_from_arg = function->getArg(2);
        capture_starts_arg = function->getArg(3);
        capture_ends_arg = function->getArg(4);

        auto * entry = newBB("entry");
        b.SetInsertPoint(entry);

        /// `$` is handled as a trailing operation so that it participates in normal backtracking.
        top_ops = program.ops;
        if (program.anchored_end)
        {
            Op suffix;
            suffix.kind = OpKind::SuffixAnchor;
            top_ops.push_back(std::move(suffix));
        }

        auto * ret0 = newBB("no_match");
        auto * match_entry = b.GetInsertBlock();
        Cont top{&top_ops, 0, nullptr};

        if (program.anchored_start)
        {
            /// `^` can only match at `begin`, so a search that starts past `begin` finds nothing.
            auto * at_begin = b.CreateICmpEQ(search_from_arg, begin_arg);
            auto * body = newBB("anchored_body");
            b.CreateCondBr(at_begin, body, ret0);
            b.SetInsertPoint(body);
            match_start = begin_arg;
            initCaptures();
            matchAt(&top, begin_arg, ret0);
        }
        else
        {
            /// Unanchored match: try every start position from `search_from` to `end` (inclusive), leftmost wins.
            ///
            /// If the program begins with an unbounded greedy quantifier over a byte set (possibly behind
            /// leading zero-width operations - capture markers or empty optionals like `()?` / `(?:)?` -
            /// which do not move the cursor), a failed attempt at `start` also fails at every position
            /// strictly inside the consumed run: at an interior start the quantifier consumes the same run
            /// end, and the set of give-back positions it tries is a subset of those already tried (and
            /// rejected) at `start`. So the search can resume at that run's end instead of `start + 1`.
            /// Without this, patterns like `[0-9]+a` or `()?[0-9]+a` on non-matching input rescan the run at
            /// every offset and are quadratic. This shortcut is sound only for `max == UNBOUNDED`: a finite
            /// `max` can still match by aligning its bounded window later in the run, so the bounded case
            /// relies on the `cursor + max` scan cap in `emitCharQuant` instead.
            for (const Op & op : top_ops)
            {
                if (isZeroWidthOp(op))
                    continue; /// does not move the cursor, so it does not affect the run the quantifier consumes
                if (op.kind == OpKind::CharQuant && op.max == UNBOUNDED)
                    leading_quant_op = &op;
                break; /// the first cursor-advancing operation decides
            }

            auto * loop = newBB("start_loop");
            b.CreateBr(loop);
            b.SetInsertPoint(loop);

            auto * start = b.CreatePHI(i8_ptr, 2);
            start->addIncoming(search_from_arg, match_entry);

            auto * past_end = b.CreateICmpUGT(start, end_arg);
            auto * body = newBB("start_body");
            b.CreateCondBr(past_end, ret0, body);

            b.SetInsertPoint(body);
            match_start = start;
            initCaptures();
            leading_run_end = nullptr;
            auto * attempt_fail = newBB("attempt_fail");
            matchAt(&top, start, attempt_fail);

            b.SetInsertPoint(attempt_fail);
            auto * start_plus_one = gepByte(start, 1);
            llvm::Value * start_next = start_plus_one;
            if (leading_quant_op && leading_run_end)
            {
                /// Resume at the leading quantifier's run end, but always advance by at least one byte:
                /// an empty run leaves `leading_run_end == start`, which would otherwise loop forever.
                auto * past = b.CreateICmpUGT(leading_run_end, start);
                start_next = b.CreateSelect(past, leading_run_end, start_plus_one);
            }
            b.CreateBr(loop);
            start->addIncoming(start_next, attempt_fail);
        }

        b.SetInsertPoint(ret0);
        b.CreateRet(b.getInt8(0));

        std::string verify_error;
        llvm::raw_string_ostream os(verify_error);
        if (llvm::verifyFunction(*function, &os))
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "JIT regexp matcher produced invalid IR: {}", verify_error);
    }

private:
    /// A frame of the continuation: "match `ops[index..]`, then continue with `next`".
    struct Cont
    {
        const std::vector<Op> * ops = nullptr;
        size_t index = 0;
        const Cont * next = nullptr;
    };

    llvm::Module & module;
    llvm::LLVMContext & context;
    llvm::IRBuilder<> b;
    const RegexpProgram & program;

    llvm::Function * function = nullptr;
    llvm::Value * begin_arg = nullptr;
    llvm::Value * end_arg = nullptr;
    llvm::Value * search_from_arg = nullptr;
    llvm::Value * capture_starts_arg = nullptr;
    llvm::Value * capture_ends_arg = nullptr;
    llvm::Value * match_start = nullptr;
    std::vector<Op> top_ops;

    /// For an unanchored search whose program begins with an unbounded greedy quantifier over a byte
    /// set: the leading op (so a failed attempt can resume past its consumed run) and that run's end
    /// at the current start position. See `emit` and `emitCharQuant`.
    const Op * leading_quant_op = nullptr;
    llvm::Value * leading_run_end = nullptr;

    llvm::PointerType * bytePtrTy() { return b.getInt8Ty()->getPointerTo(); }
    llvm::BasicBlock * newBB(const char * name) { return llvm::BasicBlock::Create(context, name, function); }
    llvm::Value * loadByte(llvm::Value * p) { return b.CreateLoad(b.getInt8Ty(), p); }
    llvm::Value * gepByte(llvm::Value * p, int64_t off)
    {
        /// Deliberately not `inbounds`: callers (`emitLiteral`, `emitCharQuant`) form a pointer past the
        /// input and only then range-check it against `end_arg`. An `inbounds` GEP that leaves the
        /// allocated object is poison, so the subsequent comparison could be miscompiled.
        return b.CreateGEP(b.getInt8Ty(), p, llvm::ConstantInt::getSigned(b.getInt64Ty(), off));
    }
    llvm::Value * ptrDiff(llvm::Value * hi, llvm::Value * lo) { return b.CreatePtrDiff(b.getInt8Ty(), hi, lo); }

    void storeCapture(llvm::Value * array_arg, int index, llvm::Value * value)
    {
        auto * elem = b.CreateInBoundsGEP(bytePtrTy(), array_arg, b.getInt32(index));
        b.CreateStore(value, elem);
    }

    void initCaptures()
    {
        auto * null_ptr = llvm::ConstantPointerNull::get(bytePtrTy());
        for (int g = 0; g < program.num_captures; ++g)
        {
            storeCapture(capture_starts_arg, g, null_ptr);
            storeCapture(capture_ends_arg, g, null_ptr);
        }
    }

    void collectCaptureIndices(const std::vector<Op> & ops, std::vector<int> & out)
    {
        for (const auto & op : ops)
        {
            if (op.kind == OpKind::CaptureStart)
                out.push_back(op.capture_index);
            else if (op.kind == OpKind::Optional)
                collectCaptureIndices(op.body, out);
        }
    }

    /// Compare a loaded byte against a literal byte, folding ASCII case when the pattern is case-insensitive.
    llvm::Value * byteEquals(llvm::Value * loaded, uint8_t literal_byte)
    {
        bool is_letter = (literal_byte >= 'a' && literal_byte <= 'z') || (literal_byte >= 'A' && literal_byte <= 'Z');
        if (program.case_insensitive && is_letter)
        {
            auto * folded = b.CreateOr(loaded, b.getInt8(0x20));
            return b.CreateICmpEQ(folded, b.getInt8(static_cast<uint8_t>(literal_byte | 0x20)));
        }
        return b.CreateICmpEQ(loaded, b.getInt8(literal_byte));
    }

    llvm::GlobalVariable * bakeSetTable(const CharSet & set)
    {
        auto * arr_type = llvm::ArrayType::get(b.getInt8Ty(), 256);
        std::vector<llvm::Constant *> values(256);
        for (unsigned c = 0; c < 256; ++c)
            values[c] = b.getInt8(set.contains(static_cast<uint8_t>(c)) ? 1 : 0);
        auto * init = llvm::ConstantArray::get(arr_type, values);
        return new llvm::GlobalVariable(
            module, arr_type, /* isConstant */ true, llvm::GlobalValue::PrivateLinkage, init, "regexp_set");
    }

    static std::vector<uint8_t> listMembers(const CharSet & set, bool complement)
    {
        std::vector<uint8_t> members;
        for (unsigned c = 0; c < 256; ++c)
            if (set.contains(static_cast<uint8_t>(c)) != complement)
                members.push_back(static_cast<uint8_t>(c));
        return members;
    }

    /// Emit a scan that consumes the maximal run of bytes that are in `set`, starting at `cursor`,
    /// stopping at the first byte not in `set` or at `scan_limit` (which callers set to `end` for an
    /// unbounded quantifier, or to `cursor + max` for a bounded one so the scan does no more work than
    /// the cap allows). Leaves the insert point at a fresh block where the returned value (a pointer to
    /// the stop position) is available.
    llvm::Value * emitConsumeRun(llvm::Value * cursor, const CharSet & set, llvm::Value * scan_limit)
    {
        size_t in_count = set.count();
        if (in_count == 0)
            return cursor;
        if (in_count == 256)
            return scan_limit; /// Every byte is in the set, so the run extends all the way to `scan_limit`.

        /// Pick the cheaper set to test with SIMD: the members themselves, or the stop set (complement).
        bool stop_when_member = false;
        std::vector<uint8_t> members;
        bool use_simd = true;
        if (in_count <= SIMD_MEMBERS_MAX)
        {
            members = listMembers(set, /* complement */ false);
            stop_when_member = false; /// stop at the first byte that is NOT one of the members.
        }
        else if (256 - in_count <= SIMD_MEMBERS_MAX)
        {
            members = listMembers(set, /* complement */ true);
            stop_when_member = true; /// stop at the first byte that IS a stop member.
        }
        else
        {
            use_simd = false;
        }

        auto * set_table = bakeSetTable(set);
        auto * vec_type = llvm::FixedVectorType::get(b.getInt8Ty(), 16);

        auto * pre = b.GetInsertBlock();
        auto * loop = newBB("scan_loop");
        auto * scalar_step = newBB("scan_scalar");
        auto * scalar_test = newBB("scan_test");
        auto * advance1 = newBB("scan_adv1");
        auto * done = newBB("scan_done");

        llvm::BasicBlock * simd_body = nullptr;
        llvm::BasicBlock * simd_hit = nullptr;
        llvm::BasicBlock * advance16 = nullptr;
        if (use_simd)
        {
            simd_body = newBB("scan_simd");
            simd_hit = newBB("scan_simd_hit");
            advance16 = newBB("scan_adv16");
        }

        b.CreateBr(loop);
        b.SetInsertPoint(loop);
        auto * p = b.CreatePHI(bytePtrTy(), 3);
        p->addIncoming(cursor, pre);

        auto * done_phi_block = done;
        b.SetInsertPoint(done);
        auto * run_end = b.CreatePHI(bytePtrTy(), use_simd ? 3 : 2);
        b.SetInsertPoint(loop);

        if (use_simd)
        {
            auto * remaining = ptrDiff(scan_limit, p);
            auto * has16 = b.CreateICmpUGE(remaining, b.getInt64(16));
            b.CreateCondBr(has16, simd_body, scalar_step);

            b.SetInsertPoint(simd_body);
            auto * vec = b.CreateAlignedLoad(vec_type, p, llvm::Align(1));
            llvm::Value * match_vec = nullptr;
            for (uint8_t m : members)
            {
                auto * splat = b.CreateVectorSplat(16, b.getInt8(m));
                auto * eq = b.CreateICmpEQ(vec, splat);
                match_vec = match_vec ? b.CreateOr(match_vec, eq) : eq;
            }
            auto * match_mask = b.CreateBitCast(match_vec, b.getInt16Ty());
            auto * stop_mask = stop_when_member ? match_mask : b.CreateNot(match_mask);
            auto * any_stop = b.CreateICmpNE(stop_mask, b.getInt16(0));
            b.CreateCondBr(any_stop, simd_hit, advance16);

            b.SetInsertPoint(simd_hit);
            auto * cttz = llvm::Intrinsic::getOrInsertDeclaration(&module, llvm::Intrinsic::cttz, {b.getInt16Ty()});
            auto * tz = b.CreateCall(cttz, {stop_mask, b.getTrue()});
            auto * hit = b.CreateInBoundsGEP(b.getInt8Ty(), p, b.CreateZExt(tz, b.getInt64Ty()));
            run_end->addIncoming(hit, simd_hit);
            b.CreateBr(done);

            b.SetInsertPoint(advance16);
            auto * p16 = gepByte(p, 16);
            b.CreateBr(loop);
            p->addIncoming(p16, advance16);
        }
        else
        {
            b.CreateBr(scalar_step);
        }

        b.SetInsertPoint(scalar_step);
        auto * at_end = b.CreateICmpEQ(p, scan_limit);
        run_end->addIncoming(p, scalar_step);
        b.CreateCondBr(at_end, done, scalar_test);

        b.SetInsertPoint(scalar_test);
        auto * byte = loadByte(p);
        auto * table_elem = b.CreateInBoundsGEP(
            llvm::ArrayType::get(b.getInt8Ty(), 256), set_table, {b.getInt64(0), b.CreateZExt(byte, b.getInt64Ty())});
        auto * in_set = b.CreateICmpNE(loadByte(table_elem), b.getInt8(0));
        run_end->addIncoming(p, scalar_test);
        b.CreateCondBr(in_set, advance1, done);

        b.SetInsertPoint(advance1);
        auto * p1 = gepByte(p, 1);
        b.CreateBr(loop);
        p->addIncoming(p1, advance1);

        b.SetInsertPoint(done_phi_block);
        return run_end;
    }

    void emitLiteral(const Op & op, llvm::Value * cursor, const Cont * rest, llvm::BasicBlock * fail)
    {
        size_t len = op.literal.size();
        auto * cursor_after = gepByte(cursor, static_cast<int64_t>(len));
        auto * in_bounds = b.CreateICmpULE(cursor_after, end_arg);
        auto * compare = newBB("lit_compare");
        b.CreateCondBr(in_bounds, compare, fail);

        b.SetInsertPoint(compare);
        llvm::Value * all_equal = b.getTrue();
        for (size_t i = 0; i < len; ++i)
        {
            auto * loaded = loadByte(gepByte(cursor, static_cast<int64_t>(i)));
            all_equal = b.CreateAnd(all_equal, byteEquals(loaded, op.literal[i]));
        }
        auto * matched = newBB("lit_ok");
        b.CreateCondBr(all_equal, matched, fail);

        b.SetInsertPoint(matched);
        matchAt(rest, cursor_after, fail);
    }

    void emitCharQuant(const Op & op, llvm::Value * cursor, const Cont * rest, llvm::BasicBlock * fail)
    {
        /// For a finite `max`, cap the run scan at `cursor + max`. Scanning the whole maximal run first
        /// and only then clamping to `max` is what makes iterative callers (`extractAll`,
        /// `replaceRegexpAll`) with bounded repeats quadratic, as each call rescans the full remainder.
        llvm::Value * scan_limit = end_arg;
        if (op.max != UNBOUNDED)
        {
            auto * max_ptr = gepByte(cursor, static_cast<int64_t>(op.max));
            scan_limit = b.CreateSelect(b.CreateICmpUGT(max_ptr, end_arg), end_arg, max_ptr);
        }

        llvm::Value * run_end = emitConsumeRun(cursor, op.set, scan_limit);

        /// A leading unbounded quantifier lets a failed unanchored attempt skip its whole consumed run
        /// at once (see `emit`); record where that run ends at this start position.
        if (&op == leading_quant_op)
            leading_run_end = run_end;

        /// `run_end` already honors `scan_limit == cursor + max`, so it is the greedy stop position.
        llvm::Value * hi = run_end;

        if (op.min > 0)
        {
            auto * count = ptrDiff(hi, cursor);
            auto * enough = b.CreateICmpUGE(count, b.getInt64(op.min));
            auto * ok = newBB("cq_min_ok");
            b.CreateCondBr(enough, ok, fail);
            b.SetInsertPoint(ok);
        }

        if (op.min == op.max || op.deterministic)
        {
            /// No backtracking needed: a fixed count, or a quantifier whose stop is unambiguous
            /// (disjoint successor / right-anchored). The rest can only match at the greedy stop `hi`.
            matchAt(rest, hi, fail);
            return;
        }

        /// Greedy give-back: try the rest of the program at `hi`, then at `hi - 1`, ... down to `cursor + min`.
        auto * lower = gepByte(cursor, static_cast<int64_t>(op.min));
        auto * pre = b.GetInsertBlock();
        auto * try_bb = newBB("gb_try");
        auto * back_bb = newBB("gb_back");
        auto * dec_bb = newBB("gb_dec");

        b.CreateBr(try_bb);
        b.SetInsertPoint(try_bb);
        auto * pos = b.CreatePHI(bytePtrTy(), 2);
        pos->addIncoming(hi, pre);
        matchAt(rest, pos, back_bb);

        b.SetInsertPoint(back_bb);
        auto * exhausted = b.CreateICmpULE(pos, lower);
        b.CreateCondBr(exhausted, fail, dec_bb);

        b.SetInsertPoint(dec_bb);
        auto * pos_next = gepByte(pos, -1);
        b.CreateBr(try_bb);
        pos->addIncoming(pos_next, dec_bb);
    }

    void emitOptional(const Op & op, llvm::Value * cursor, const Cont * rest, llvm::BasicBlock * fail)
    {
        auto * skip = newBB("opt_skip");
        Cont body_cont{&op.body, 0, rest};
        /// Greedy: prefer matching the body (and the rest); fall back to skipping the body.
        matchAt(&body_cont, cursor, skip);

        b.SetInsertPoint(skip);
        /// The optional group did not participate, so its captures are unset.
        std::vector<int> indices;
        collectCaptureIndices(op.body, indices);
        auto * null_ptr = llvm::ConstantPointerNull::get(bytePtrTy());
        for (int index : indices)
        {
            storeCapture(capture_starts_arg, index, null_ptr);
            storeCapture(capture_ends_arg, index, null_ptr);
        }
        matchAt(rest, cursor, fail);
    }

    void emitSuccess(llvm::Value * cursor)
    {
        storeCapture(capture_starts_arg, 0, match_start);
        storeCapture(capture_ends_arg, 0, cursor);
        b.CreateRet(b.getInt8(1));
    }

    void matchAt(const Cont * cont, llvm::Value * cursor, llvm::BasicBlock * fail)
    {
        if (!cont)
        {
            emitSuccess(cursor);
            return;
        }
        if (cont->index >= cont->ops->size())
        {
            matchAt(cont->next, cursor, fail);
            return;
        }

        const Op & op = (*cont->ops)[cont->index];
        Cont rest{cont->ops, cont->index + 1, cont->next};

        switch (op.kind)
        {
            case OpKind::CaptureStart:
                storeCapture(capture_starts_arg, op.capture_index, cursor);
                matchAt(&rest, cursor, fail);
                break;
            case OpKind::CaptureEnd:
                storeCapture(capture_ends_arg, op.capture_index, cursor);
                matchAt(&rest, cursor, fail);
                break;
            case OpKind::SuffixAnchor:
            {
                auto * is_end = b.CreateICmpEQ(cursor, end_arg);
                auto * ok = newBB("suffix_ok");
                b.CreateCondBr(is_end, ok, fail);
                b.SetInsertPoint(ok);
                matchAt(&rest, cursor, fail);
                break;
            }
            case OpKind::PrefixAnchor:
            {
                auto * is_begin = b.CreateICmpEQ(cursor, begin_arg);
                auto * ok = newBB("prefix_ok");
                b.CreateCondBr(is_begin, ok, fail);
                b.SetInsertPoint(ok);
                matchAt(&rest, cursor, fail);
                break;
            }
            case OpKind::Literal:
                emitLiteral(op, cursor, &rest, fail);
                break;
            case OpKind::CharQuant:
                emitCharQuant(op, cursor, &rest, fail);
                break;
            case OpKind::Optional:
                emitOptional(op, cursor, &rest, fail);
                break;
        }
    }
};

UInt128 computeKey(const std::string & pattern, bool case_insensitive, bool dot_all)
{
    SipHash hash;
    hash.update(pattern.data(), pattern.size());
    hash.update(case_insensitive);
    hash.update(dot_all);
    return hash.get128();
}

std::shared_ptr<CompiledRegexpHolder> compileMatcher(const RegexpProgram & program)
{
    auto jit = getRegexpJITInstancePtr();
    auto compiled_module = jit->compileModule([&](llvm::Module & module)
    {
        RegexpEmitter emitter(module, program);
        emitter.emit();
    });

    auto it = compiled_module.function_name_to_symbol.find("regexp_match");
    if (it == compiled_module.function_name_to_symbol.end())
    {
        jit->deleteCompiledModule(compiled_module);
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Compiled regexp module has no `regexp_match` symbol");
    }

    auto func = reinterpret_cast<JITRegexpMatcherFunc>(it->second);

    /// `compileModule` has already registered the module in the JIT instance; it is released only by
    /// `deleteCompiledModule` (called from `~CompiledRegexpHolder`). Until the holder is constructed and
    /// takes ownership, there is an unmanaged gap: if `make_shared` throws (e.g. `bad_alloc`), the module
    /// would stay mapped until a full reset of the regexp JIT instance. Delete it explicitly on that path.
    /// The raw pointer stays valid even after `jit` is moved from, because `regexp_jit_instance` keeps the
    /// `CHJIT` alive.
    CHJIT * jit_ptr = jit.get();
    try
    {
        return std::make_shared<CompiledRegexpHolder>(compiled_module, std::move(jit), func, program.num_captures);
    }
    catch (...)
    {
        jit_ptr->deleteCompiledModule(compiled_module);
        throw;
    }
}

}

RegexpJITMatcher getRegexpJITMatcher(
    const std::string & pattern, bool case_insensitive, bool dot_all, size_t min_count_to_compile)
{
    /// `std::numeric_limits<size_t>::max()` is the disabled sentinel (`compile_regular_expressions = 0`).
    /// Return before doing any work, so a disabled JIT neither parses patterns nor grows the seen-count
    /// map - some callers (`extractAll`, `replaceRegexp*`) invoke this unconditionally.
    if (min_count_to_compile == std::numeric_limits<size_t>::max())
        return {};

    ParseFlags flags;
    flags.case_insensitive = case_insensitive;
    flags.dot_all = dot_all;

    auto program = tryCompileToProgram(pattern, flags);
    if (!program)
        return {};

    const UInt128 key = computeKey(pattern, case_insensitive, dot_all);

    {
        std::lock_guard lock(regexp_seen_counter_mutex);
        /// Keep the map bounded: if it is full and this is a new key, drop everything and start over.
        /// Resetting the counts only delays compilation of the patterns in flight, which is harmless.
        if (regexp_seen_counter.size() >= MAX_SEEN_COUNTER_ENTRIES && !regexp_seen_counter.contains(key))
            regexp_seen_counter.clear();
        if (regexp_seen_counter[key]++ < min_count_to_compile)
            return {};
    }

    std::shared_ptr<CompiledRegexpHolder> holder;
    try
    {
        if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
        {
            auto [entry, _] = cache->getOrSet(key, [&]() -> std::shared_ptr<CompiledExpressionCacheEntry>
            {
                return compileMatcher(*program);
            });
            holder = std::static_pointer_cast<CompiledRegexpHolder>(entry);
        }
        else
        {
            holder = compileMatcher(*program);
        }
    }
    catch (...)
    {
        /// A failure to compile is not a correctness problem - the caller falls back to RE2.
        tryLogCurrentException(getLogger("CompileRegexp"), "Failed to JIT-compile a regular expression, falling back to RE2");
        return {};
    }

    RegexpJITMatcher result;
    result.func = holder->func;
    result.num_captures = holder->num_captures;
    result.keep_alive = std::move(holder);
    return result;
}

}

#else

namespace DB
{

RegexpJITMatcher getRegexpJITMatcher(const std::string &, bool, bool, size_t)
{
    return {};
}

void resetRegexpJITInstance()
{
}

}

#endif
