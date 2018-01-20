// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tested by search_test.cc.
//
// Prog::SearchOnePass is an efficient implementation of
// regular expression search with submatch tracking for
// what I call "one-pass regular expressions".  (An alternate
// name might be "backtracking-free regular expressions".)
//
// One-pass regular expressions have the property that
// at each input byte during an anchored match, there may be
// multiple alternatives but only one can proceed for any
// given input byte.
//
// For example, the regexp /x*yx*/ is one-pass: you read
// x's until a y, then you read the y, then you keep reading x's.
// At no point do you have to guess what to do or back up
// and try a different guess.
//
// On the other hand, /x*x/ is not one-pass: when you're
// looking at an input "x", it's not clear whether you should
// use it to extend the x* or as the final x.
//
// More examples: /([^ ]*) (.*)/ is one-pass; /(.*) (.*)/ is not.
// /(\d+)-(\d+)/ is one-pass; /(\d+).(\d+)/ is not.
//
// A simple intuition for identifying one-pass regular expressions
// is that it's always immediately obvious when a repetition ends.
// It must also be immediately obvious which branch of an | to take:
//
// /x(y|z)/ is one-pass, but /(xy|xz)/ is not.
//
// The NFA-based search in nfa.cc does some bookkeeping to
// avoid the need for backtracking and its associated exponential blowup.
// But if we have a one-pass regular expression, there is no
// possibility of backtracking, so there is no need for the
// extra bookkeeping.  Hence, this code.
//
// On a one-pass regular expression, the NFA code in nfa.cc
// runs at about 1/20 of the backtracking-based PCRE speed.
// In contrast, the code in this file runs at about the same
// speed as PCRE.
//
// One-pass regular expressions get used a lot when RE is
// used for parsing simple strings, so it pays off to
// notice them and handle them efficiently.
//
// See also Anne Brüggemann-Klein and Derick Wood,
// "One-unambiguous regular languages", Information and Computation 142(2).

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "util/util.h"
#include "util/logging.h"
#include "util/sparse_set.h"
#include "util/strutil.h"
#include "util/utf.h"
#include "re2/prog.h"
#include "re2/stringpiece.h"

// Silence "zero-sized array in struct/union" warning for OneState::action.
#ifdef _MSC_VER
#pragma warning(disable: 4200)
#endif

namespace re2 {

static const bool ExtraDebug = false;

// The key insight behind this implementation is that the
// non-determinism in an NFA for a one-pass regular expression
// is contained.  To explain what that means, first a
// refresher about what regular expression programs look like
// and how the usual NFA execution runs.
//
// In a regular expression program, only the kInstByteRange
// instruction processes an input byte c and moves on to the
// next byte in the string (it does so if c is in the given range).
// The kInstByteRange instructions correspond to literal characters
// and character classes in the regular expression.
//
// The kInstAlt instructions are used as wiring to connect the
// kInstByteRange instructions together in interesting ways when
// implementing | + and *.
// The kInstAlt instruction forks execution, like a goto that
// jumps to ip->out() and ip->out1() in parallel.  Each of the
// resulting computation paths is called a thread.
//
// The other instructions -- kInstEmptyWidth, kInstMatch, kInstCapture --
// are interesting in their own right but like kInstAlt they don't
// advance the input pointer.  Only kInstByteRange does.
//
// The automaton execution in nfa.cc runs all the possible
// threads of execution in lock-step over the input.  To process
// a particular byte, each thread gets run until it either dies
// or finds a kInstByteRange instruction matching the byte.
// If the latter happens, the thread stops just past the
// kInstByteRange instruction (at ip->out()) and waits for
// the other threads to finish processing the input byte.
// Then, once all the threads have processed that input byte,
// the whole process repeats.  The kInstAlt state instruction
// might create new threads during input processing, but no
// matter what, all the threads stop after a kInstByteRange
// and wait for the other threads to "catch up".
// Running in lock step like this ensures that the NFA reads
// the input string only once.
//
// Each thread maintains its own set of capture registers
// (the string positions at which it executed the kInstCapture
// instructions corresponding to capturing parentheses in the
// regular expression).  Repeated copying of the capture registers
// is the main performance bottleneck in the NFA implementation.
//
// A regular expression program is "one-pass" if, no matter what
// the input string, there is only one thread that makes it
// past a kInstByteRange instruction at each input byte.  This means
// that there is in some sense only one active thread throughout
// the execution.  Other threads might be created during the
// processing of an input byte, but they are ephemeral: only one
// thread is left to start processing the next input byte.
// This is what I meant above when I said the non-determinism
// was "contained".
//
// To execute a one-pass regular expression program, we can build
// a DFA (no non-determinism) that has at most as many states as
// the NFA (compare this to the possibly exponential number of states
// in the general case).  Each state records, for each possible
// input byte, the next state along with the conditions required
// before entering that state -- empty-width flags that must be true
// and capture operations that must be performed.  It also records
// whether a set of conditions required to finish a match at that
// point in the input rather than process the next byte.

// A state in the one-pass NFA - just an array of actions indexed
// by the bytemap_[] of the next input byte.  (The bytemap
// maps next input bytes into equivalence classes, to reduce
// the memory footprint.)
struct OneState {
  uint32_t matchcond;   // conditions to match right now.
  uint32_t action[];
};

// The uint32_t conditions in the action are a combination of
// condition and capture bits and the next state.  The bottom 16 bits
// are the condition and capture bits, and the top 16 are the index of
// the next state.
//
// Bits 0-5 are the empty-width flags from prog.h.
// Bit 6 is kMatchWins, which means the match takes
// priority over moving to next in a first-match search.
// The remaining bits mark capture registers that should
// be set to the current input position.  The capture bits
// start at index 2, since the search loop can take care of
// cap[0], cap[1] (the overall match position).
// That means we can handle up to 5 capturing parens: $1 through $4, plus $0.
// No input position can satisfy both kEmptyWordBoundary
// and kEmptyNonWordBoundary, so we can use that as a sentinel
// instead of needing an extra bit.

static const int    kIndexShift   = 16;  // number of bits below index
static const int    kEmptyShift   = 6;   // number of empty flags in prog.h
static const int    kRealCapShift = kEmptyShift + 1;
static const int    kRealMaxCap   = (kIndexShift - kRealCapShift) / 2 * 2;

// Parameters used to skip over cap[0], cap[1].
static const int    kCapShift     = kRealCapShift - 2;
static const int    kMaxCap       = kRealMaxCap + 2;

static const uint32_t kMatchWins  = 1 << kEmptyShift;
static const uint32_t kCapMask    = ((1 << kRealMaxCap) - 1) << kRealCapShift;

static const uint32_t kImpossible = kEmptyWordBoundary | kEmptyNonWordBoundary;

// Check, at compile time, that prog.h agrees with math above.
// This function is never called.
void OnePass_Checks() {
  static_assert((1<<kEmptyShift)-1 == kEmptyAllFlags,
                "kEmptyShift disagrees with kEmptyAllFlags");
  // kMaxCap counts pointers, kMaxOnePassCapture counts pairs.
  static_assert(kMaxCap == Prog::kMaxOnePassCapture*2,
                "kMaxCap disagrees with kMaxOnePassCapture");
}

static bool Satisfy(uint32_t cond, const StringPiece& context, const char* p) {
  uint32_t satisfied = Prog::EmptyFlags(context, p);
  if (cond & kEmptyAllFlags & ~satisfied)
    return false;
  return true;
}

// Apply the capture bits in cond, saving p to the appropriate
// locations in cap[].
static void ApplyCaptures(uint32_t cond, const char* p,
                          const char** cap, int ncap) {
  for (int i = 2; i < ncap; i++)
    if (cond & (1 << kCapShift << i))
      cap[i] = p;
}

// Computes the OneState* for the given nodeindex.
static inline OneState* IndexToNode(uint8_t* nodes, int statesize,
                                    int nodeindex) {
  return reinterpret_cast<OneState*>(nodes + statesize*nodeindex);
}

bool Prog::SearchOnePass(const StringPiece& text,
                         const StringPiece& const_context,
                         Anchor anchor, MatchKind kind,
                         StringPiece* match, int nmatch) {
  if (anchor != kAnchored && kind != kFullMatch) {
    LOG(DFATAL) << "Cannot use SearchOnePass for unanchored matches.";
    return false;
  }

  // Make sure we have at least cap[1],
  // because we use it to tell if we matched.
  int ncap = 2*nmatch;
  if (ncap < 2)
    ncap = 2;

  const char* cap[kMaxCap];
  for (int i = 0; i < ncap; i++)
    cap[i] = NULL;

  const char* matchcap[kMaxCap];
  for (int i = 0; i < ncap; i++)
    matchcap[i] = NULL;

  StringPiece context = const_context;
  if (context.begin() == NULL)
    context = text;
  if (anchor_start() && context.begin() != text.begin())
    return false;
  if (anchor_end() && context.end() != text.end())
    return false;
  if (anchor_end())
    kind = kFullMatch;

  uint8_t* nodes = onepass_nodes_;
  int statesize = sizeof(OneState) + bytemap_range()*sizeof(uint32_t);
  // start() is always mapped to the zeroth OneState.
  OneState* state = IndexToNode(nodes, statesize, 0);
  uint8_t* bytemap = bytemap_;
  const char* bp = text.begin();
  const char* ep = text.end();
  const char* p;
  bool matched = false;
  matchcap[0] = bp;
  cap[0] = bp;
  uint32_t nextmatchcond = state->matchcond;
  for (p = bp; p < ep; p++) {
    int c = bytemap[*p & 0xFF];
    uint32_t matchcond = nextmatchcond;
    uint32_t cond = state->action[c];

    // Determine whether we can reach act->next.
    // If so, advance state and nextmatchcond.
    if ((cond & kEmptyAllFlags) == 0 || Satisfy(cond, context, p)) {
      uint32_t nextindex = cond >> kIndexShift;
      state = IndexToNode(nodes, statesize, nextindex);
      nextmatchcond = state->matchcond;
    } else {
      state = NULL;
      nextmatchcond = kImpossible;
    }

    // This code section is carefully tuned.
    // The goto sequence is about 10% faster than the
    // obvious rewrite as a large if statement in the
    // ASCIIMatchRE2 and DotMatchRE2 benchmarks.

    // Saving the match capture registers is expensive.
    // Is this intermediate match worth thinking about?

    // Not if we want a full match.
    if (kind == kFullMatch)
      goto skipmatch;

    // Not if it's impossible.
    if (matchcond == kImpossible)
      goto skipmatch;

    // Not if the possible match is beaten by the certain
    // match at the next byte.  When this test is useless
    // (e.g., HTTPPartialMatchRE2) it slows the loop by
    // about 10%, but when it avoids work (e.g., DotMatchRE2),
    // it cuts the loop execution by about 45%.
    if ((cond & kMatchWins) == 0 && (nextmatchcond & kEmptyAllFlags) == 0)
      goto skipmatch;

    // Finally, the match conditions must be satisfied.
    if ((matchcond & kEmptyAllFlags) == 0 || Satisfy(matchcond, context, p)) {
      for (int i = 2; i < 2*nmatch; i++)
        matchcap[i] = cap[i];
      if (nmatch > 1 && (matchcond & kCapMask))
        ApplyCaptures(matchcond, p, matchcap, ncap);
      matchcap[1] = p;
      matched = true;

      // If we're in longest match mode, we have to keep
      // going and see if we find a longer match.
      // In first match mode, we can stop if the match
      // takes priority over the next state for this input byte.
      // That bit is per-input byte and thus in cond, not matchcond.
      if (kind == kFirstMatch && (cond & kMatchWins))
        goto done;
    }

  skipmatch:
    if (state == NULL)
      goto done;
    if ((cond & kCapMask) && nmatch > 1)
      ApplyCaptures(cond, p, cap, ncap);
  }

  // Look for match at end of input.
  {
    uint32_t matchcond = state->matchcond;
    if (matchcond != kImpossible &&
        ((matchcond & kEmptyAllFlags) == 0 || Satisfy(matchcond, context, p))) {
      if (nmatch > 1 && (matchcond & kCapMask))
        ApplyCaptures(matchcond, p, cap, ncap);
      for (int i = 2; i < ncap; i++)
        matchcap[i] = cap[i];
      matchcap[1] = p;
      matched = true;
    }
  }

done:
  if (!matched)
    return false;
  for (int i = 0; i < nmatch; i++)
    match[i] =
        StringPiece(matchcap[2 * i],
                    static_cast<size_t>(matchcap[2 * i + 1] - matchcap[2 * i]));
  return true;
}


// Analysis to determine whether a given regexp program is one-pass.

// If ip is not on workq, adds ip to work queue and returns true.
// If ip is already on work queue, does nothing and returns false.
// If ip is NULL, does nothing and returns true (pretends to add it).
typedef SparseSet Instq;
static bool AddQ(Instq *q, int id) {
  if (id == 0)
    return true;
  if (q->contains(id))
    return false;
  q->insert(id);
  return true;
}

struct InstCond {
  int id;
  uint32_t cond;
};

// Returns whether this is a one-pass program; that is,
// returns whether it is safe to use SearchOnePass on this program.
// These conditions must be true for any instruction ip:
//
//   (1) for any other Inst nip, there is at most one input-free
//       path from ip to nip.
//   (2) there is at most one kInstByte instruction reachable from
//       ip that matches any particular byte c.
//   (3) there is at most one input-free path from ip to a kInstMatch
//       instruction.
//
// This is actually just a conservative approximation: it might
// return false when the answer is true, when kInstEmptyWidth
// instructions are involved.
// Constructs and saves corresponding one-pass NFA on success.
bool Prog::IsOnePass() {
  if (did_onepass_)
    return onepass_nodes_ != NULL;
  did_onepass_ = true;

  if (start() == 0)  // no match
    return false;

  // Steal memory for the one-pass NFA from the overall DFA budget.
  // Willing to use at most 1/4 of the DFA budget (heuristic).
  // Limit max node count to 65000 as a conservative estimate to
  // avoid overflowing 16-bit node index in encoding.
  int maxnodes = 2 + inst_count(kInstByteRange);
  int statesize = sizeof(OneState) + bytemap_range()*sizeof(uint32_t);
  if (maxnodes >= 65000 || dfa_mem_ / 4 / statesize < maxnodes)
    return false;

  // Flood the graph starting at the start state, and check
  // that in each reachable state, each possible byte leads
  // to a unique next state.
  int stacksize = inst_count(kInstCapture) +
                  inst_count(kInstEmptyWidth) +
                  inst_count(kInstNop) + 1;  // + 1 for start inst
  InstCond* stack = new InstCond[stacksize];

  int size = this->size();
  int* nodebyid = new int[size];  // indexed by ip
  memset(nodebyid, 0xFF, size*sizeof nodebyid[0]);

  // Originally, nodes was a uint8_t[maxnodes*statesize], but that was
  // unnecessarily optimistic: why allocate a large amount of memory
  // upfront for a large program when it is unlikely to be one-pass?
  std::vector<uint8_t> nodes;

  Instq tovisit(size), workq(size);
  AddQ(&tovisit, start());
  nodebyid[start()] = 0;
  int nalloc = 1;
  nodes.insert(nodes.end(), statesize, 0);
  for (Instq::iterator it = tovisit.begin(); it != tovisit.end(); ++it) {
    int id = *it;
    int nodeindex = nodebyid[id];
    OneState* node = IndexToNode(nodes.data(), statesize, nodeindex);

    // Flood graph using manual stack, filling in actions as found.
    // Default is none.
    for (int b = 0; b < bytemap_range_; b++)
      node->action[b] = kImpossible;
    node->matchcond = kImpossible;

    workq.clear();
    bool matched = false;
    int nstack = 0;
    stack[nstack].id = id;
    stack[nstack++].cond = 0;
    while (nstack > 0) {
      int id = stack[--nstack].id;
      uint32_t cond = stack[nstack].cond;

    Loop:
      Prog::Inst* ip = inst(id);
      switch (ip->opcode()) {
        default:
          LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
          break;

        case kInstAltMatch:
          // TODO(rsc): Ignoring kInstAltMatch optimization.
          // Should implement it in this engine, but it's subtle.
          DCHECK(!ip->last());
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;

        case kInstByteRange: {
          int nextindex = nodebyid[ip->out()];
          if (nextindex == -1) {
            if (nalloc >= maxnodes) {
              if (ExtraDebug)
                LOG(ERROR) << StringPrintf(
                    "Not OnePass: hit node limit %d >= %d", nalloc, maxnodes);
              goto fail;
            }
            nextindex = nalloc;
            AddQ(&tovisit, ip->out());
            nodebyid[ip->out()] = nalloc;
            nalloc++;
            nodes.insert(nodes.end(), statesize, 0);
            // Update node because it might have been invalidated.
            node = IndexToNode(nodes.data(), statesize, nodeindex);
          }
          for (int c = ip->lo(); c <= ip->hi(); c++) {
            int b = bytemap_[c];
            // Skip any bytes immediately after c that are also in b.
            while (c < 256-1 && bytemap_[c+1] == b)
              c++;
            uint32_t act = node->action[b];
            uint32_t newact = (nextindex << kIndexShift) | cond;
            if (matched)
              newact |= kMatchWins;
            if ((act & kImpossible) == kImpossible) {
              node->action[b] = newact;
            } else if (act != newact) {
              if (ExtraDebug)
                LOG(ERROR) << StringPrintf(
                    "Not OnePass: conflict on byte %#x at state %d", c, *it);
              goto fail;
            }
          }
          if (ip->foldcase()) {
            Rune lo = std::max<Rune>(ip->lo(), 'a') + 'A' - 'a';
            Rune hi = std::min<Rune>(ip->hi(), 'z') + 'A' - 'a';
            for (int c = lo; c <= hi; c++) {
              int b = bytemap_[c];
              // Skip any bytes immediately after c that are also in b.
              while (c < 256-1 && bytemap_[c+1] == b)
                c++;
              uint32_t act = node->action[b];
              uint32_t newact = (nextindex << kIndexShift) | cond;
              if (matched)
                newact |= kMatchWins;
              if ((act & kImpossible) == kImpossible) {
                node->action[b] = newact;
              } else if (act != newact) {
                if (ExtraDebug)
                  LOG(ERROR) << StringPrintf(
                      "Not OnePass: conflict on byte %#x at state %d", c, *it);
                goto fail;
              }
            }
          }

          if (ip->last())
            break;
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;
        }

        case kInstCapture:
        case kInstEmptyWidth:
        case kInstNop:
          if (!ip->last()) {
            // If already on work queue, (1) is violated: bail out.
            if (!AddQ(&workq, id+1))
              goto fail;
            stack[nstack].id = id+1;
            stack[nstack++].cond = cond;
          }

          if (ip->opcode() == kInstCapture && ip->cap() < kMaxCap)
            cond |= (1 << kCapShift) << ip->cap();
          if (ip->opcode() == kInstEmptyWidth)
            cond |= ip->empty();

          // kInstCapture and kInstNop always proceed to ip->out().
          // kInstEmptyWidth only sometimes proceeds to ip->out(),
          // but as a conservative approximation we assume it always does.
          // We could be a little more precise by looking at what c
          // is, but that seems like overkill.

          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, ip->out())) {
            if (ExtraDebug)
              LOG(ERROR) << StringPrintf(
                  "Not OnePass: multiple paths %d -> %d\n", *it, ip->out());
            goto fail;
          }
          id = ip->out();
          goto Loop;

        case kInstMatch:
          if (matched) {
            // (3) is violated
            if (ExtraDebug)
              LOG(ERROR) << StringPrintf(
                  "Not OnePass: multiple matches from %d\n", *it);
            goto fail;
          }
          matched = true;
          node->matchcond = cond;

          if (ip->last())
            break;
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;

        case kInstFail:
          break;
      }
    }
  }

  if (ExtraDebug) {  // For debugging, dump one-pass NFA to LOG(ERROR).
    LOG(ERROR) << "bytemap:\n" << DumpByteMap();
    LOG(ERROR) << "prog:\n" << Dump();

    std::map<int, int> idmap;
    for (int i = 0; i < size; i++)
      if (nodebyid[i] != -1)
        idmap[nodebyid[i]] = i;

    string dump;
    for (Instq::iterator it = tovisit.begin(); it != tovisit.end(); ++it) {
      int id = *it;
      int nodeindex = nodebyid[id];
      if (nodeindex == -1)
        continue;
      OneState* node = IndexToNode(nodes.data(), statesize, nodeindex);
      StringAppendF(&dump, "node %d id=%d: matchcond=%#x\n",
                    nodeindex, id, node->matchcond);
      for (int i = 0; i < bytemap_range_; i++) {
        if ((node->action[i] & kImpossible) == kImpossible)
          continue;
        StringAppendF(&dump, "  %d cond %#x -> %d id=%d\n",
                      i, node->action[i] & 0xFFFF,
                      node->action[i] >> kIndexShift,
                      idmap[node->action[i] >> kIndexShift]);
      }
    }
    LOG(ERROR) << "nodes:\n" << dump;
  }

  dfa_mem_ -= nalloc*statesize;
  onepass_nodes_ = new uint8_t[nalloc*statesize];
  memmove(onepass_nodes_, nodes.data(), nalloc*statesize);

  delete[] stack;
  delete[] nodebyid;
  return true;

fail:
  delete[] stack;
  delete[] nodebyid;
  return false;
}

}  // namespace re2
