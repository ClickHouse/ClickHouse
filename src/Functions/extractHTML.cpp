#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/selectors/SelectorMatchingVM.h>
#include <Functions/selectors/TagScanner.h>
#include <Functions/selectors/parseNextAttribute.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

std::pair<const char *, MatchResult> feedAttributes(const char * begin, const char * end, SelectorMatchingVM & vm) {
    while (begin != end) {
        Attribute attribute;
        begin = parseNextAttribute(begin, end, attribute);
        if (!attribute.key.value.empty()) {
            auto match_result = vm.handleAttribute(attribute);
            switch (match_result)
            {
                case MatchResult::MATCH:
                case MatchResult::NOT_MATCH:
                    return {begin, match_result};
                case MatchResult::NEED_ATTRIBUTES:
                    continue;
            }
        }

        return {begin, MatchResult::NOT_MATCH};
    }
    return {end, MatchResult::NOT_MATCH};
}

template <bool only_first_match, typename F>
size_t extract(
    const char * __restrict src,
    size_t size,
    char * __restrict dst,
    SelectorMatchingVM & vm,
    F && copy_match)
{
    TagScanner tag_scanner;

    const char * end = src + size;
    char * dst_begin = dst;
    const char * match_start = nullptr;

    MatchResult cur_match_result = MatchResult::NOT_MATCH;
    MatchResult prev_match_result = MatchResult::NOT_MATCH;

    while (src != end)
    {
        TagPreview tag_preview;
        const char * next = tag_scanner.scan(src, end, tag_preview);

        if (next == end)
        {
            if (cur_match_result == MatchResult::MATCH && match_start != nullptr)
            {
                dst = copy_match(dst, match_start, end - match_start);
                return dst - dst_begin;
            }
            break;
        }

        if (tag_preview.is_closing)
            cur_match_result = vm.handleClosingTag(tag_preview.name);
        else
            cur_match_result = vm.handleOpeningTag(tag_preview.name);

        if (cur_match_result == MatchResult::NEED_ATTRIBUTES)
            std::tie(next, cur_match_result) = feedAttributes(next, end, vm);

        switch (cur_match_result)
        {
            case MatchResult::MATCH:
                if (!tag_preview.is_closing)
                {
                    if (prev_match_result == MatchResult::NOT_MATCH)
                        match_start = tag_scanner.last_tag_start;
                }
                break;
            case MatchResult::NOT_MATCH:
                if (tag_preview.is_closing)
                {
                    if (prev_match_result == MatchResult::MATCH && match_start != nullptr)
                    {
                        dst = copy_match(dst, match_start, next - match_start + 1);
                        if constexpr (only_first_match)
                            return dst - dst_begin;
                    }
                }
                break;
            case MatchResult::NEED_ATTRIBUTES:
                break;
        }
        prev_match_result = cur_match_result;
        src = next;
    }

    return dst - dst_begin;
}

}

template <bool only_first_match>
class FunctionExtractHTML : public IFunction
{
public:
    static constexpr auto name = only_first_match ? "extractHTMLOne" : "extractHTMLAll";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractHTML<only_first_match>>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"document", &isStringOrFixedString<IDataType>, nullptr, "const String or const FixedString"},
            {"selector", &isStringOrFixedString<IDataType>, isColumnConst, "const String or const FixedString"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        if constexpr (only_first_match)
            return std::make_shared<DataTypeString>();
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeOne(const ColumnString * src_column, size_t rows, SelectorMatchingVM & vm) const
    {
        const ColumnString::Chars & src_chars = src_column->getChars();
        const ColumnString::Offsets & src_offsets = src_column->getOffsets();

        auto res = ColumnString::create();

        ColumnString::Chars & res_chars = res->getChars();
        ColumnString::Offsets & res_offsets = res->getOffsets();

        res_chars.resize(src_chars.size());
        res_offsets.resize(src_offsets.size());

        ColumnString::Offset src_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < rows; ++i)
        {
            vm.reset();
            auto next_src_offset = src_offsets[i];

            res_offset += extract<only_first_match>(
                reinterpret_cast<const char *>(&src_chars[src_offset]),
                next_src_offset - src_offset - 1,
                reinterpret_cast<char *>(&res_chars[res_offset]),
                vm,
                [](char * dst, const char * src, size_t n){
                    memcpy(dst, src, n);
                    return dst + n;
                });

            res_chars[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            src_offset = next_src_offset;
        }

        res_chars.resize(res_offset);
        return res;
    }

    ColumnPtr executeAll(const ColumnString * src_column, size_t rows, SelectorMatchingVM & vm) const
    {
        const ColumnString::Chars & src_chars = src_column->getChars();
        const ColumnString::Offsets & src_offsets = src_column->getOffsets();

        auto res = ColumnArray::create(ColumnString::create());
        ColumnString & res_strings = typeid_cast<ColumnString &>(res->getData());

        ColumnArray::Offsets & res_offsets = res->getOffsets();
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

        ColumnString::Offset src_offset = 0;
        ColumnString::Offset res_offset = 0;

        res_strings_chars.resize(src_chars.size());
        res_offsets.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            vm.reset();
            auto next_src_offset = src_offsets[i];

            res_offset += extract<only_first_match>(
                reinterpret_cast<const char *>(&src_chars[src_offset]),
                next_src_offset - src_offset - 1,
                reinterpret_cast<char *>(&res_strings_chars[res_offset]),
                vm,
                [&res_strings_offsets, &res_strings_chars, offset = 0ul](char * dst, const char * src, size_t n) mutable
                {
                    memcpy(dst, src, n);
                    offset += n;
                    res_strings_chars[offset] = 0;
                    ++offset;
                    res_strings_offsets.push_back(offset);
                    return dst + n + 1;
                });

            ++res_offset;
            res_offsets[i] = res_strings_offsets.size();

            src_offset = next_src_offset;
        }

        return res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        const ColumnString * src = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!src)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "First argument for function {} must be string, got {}",
                getName(),
                arguments[0].column->getName());

        const ColumnConst * selector_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!selector_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Second argument for function {} must be constant, got {}",
                getName(),
                arguments[1].column->getName());

        String selector = selector_column->getValue<String>();
        SelectorMatchingVM vm = SelectorMatchingVM::parseSelector(selector.data(), selector.data() + selector.size());

        return only_first_match ? executeOne(src, rows, vm) : executeAll(src, rows, vm);
    }
};

void registerFunctionExtractHTMLAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractHTML<false>>();
}

void registerFunctionExtractHTMLOne(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractHTML<true>>();
}

}
