#pragma once

#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>

#include <string_view>


/** CSS selectors over HTML for the functions `extractHTMLOne` and `extractHTMLAll`.
  *
  * The implementation does not build a DOM tree. The document is scanned once, left to right,
  * with a fast tokenizer (similar to the one of `extractTextFromHTML`), and the selector is matched
  * on the fly against a stack of open elements.
  *
  * A selector list is compiled into a flat sequence of "steps": every compound selector
  * (like `div.item[data-id]`) of every complex selector (like `ul > li a`) is a step.
  * Every open element keeps a bit mask of the steps it matches, the union of these masks over
  * all its ancestors, and the unions over its previous siblings. Then the condition of every
  * combinator is a single shift and AND of these masks:
  *   descendant ` ` - the previous step is matched by some ancestor,
  *   child `>` - by the parent,
  *   next sibling `+` - by the previous element sibling,
  *   subsequent sibling `~` - by some previous element sibling.
  * This gives exact CSS semantics without backtracking. The number of steps is limited to 64.
  *
  * The HTML is parsed approximately, but the most important rules of the HTML tree construction
  * are taken into account: void elements (`br`, `img`, ...), self-closing tags (`<x/>`), raw text
  * elements (`script`, `style`, `textarea`, `title`, ...), comments, CDATA, implicitly closed
  * elements (`<p>`, `<li>`, `<td>`, `<tr>`, `<option>`, ...) and the scope of end tags.
  * HTML entities are not decoded.
  */

namespace DB::HTML
{

/// Condition on an attribute, also used for `#id` and `.class`.
struct AttributeCondition
{
    enum class Operator : uint8_t
    {
        Exists,         /// [attr]
        Equals,         /// [attr=value]
        Includes,       /// [attr~=value]
        DashMatch,      /// [attr|=value]
        Prefix,         /// [attr^=value]
        Suffix,         /// [attr$=value]
        Substring,      /// [attr*=value]
    };

    String name;        /// In lowercase.
    String value;
    Operator op = Operator::Exists;
    bool case_insensitive = false;

    [[nodiscard]] bool matches(std::string_view attribute_value) const;
};

/// :nth-child(a n + b)
struct NthCondition
{
    Int64 a = 0;
    Int64 b = 1;

    [[nodiscard]] bool matches(UInt64 index) const;
};

/// A compound selector, like `div#main.content[lang|=en]:not(.hidden)`.
struct CompoundSelector
{
    String tag;         /// In lowercase, empty means any element.
    VectorWithMemoryTracking<AttributeCondition> attributes;
    VectorWithMemoryTracking<NthCondition> nth_child;
    VectorWithMemoryTracking<CompoundSelector> negations;
    bool root = false;

    [[nodiscard]] bool needsAttributes() const;
};

enum class Combinator : uint8_t
{
    None,               /// The first compound selector of a complex selector.
    Descendant,
    Child,
    NextSibling,
    SubsequentSibling,
};

/// What to extract from a matched element.
enum class Output : uint8_t
{
    OuterHTML,          /// Default: the element with all its content.
    Text,               /// ::text - the text content of the element.
    Attribute,          /// ::attr(name) - the value of the attribute.
};

struct Attribute
{
    std::string_view name;
    std::string_view value;
};

/// What is known about an element when its start tag is processed.
struct ElementInfo
{
    std::string_view name;
    const VectorWithMemoryTracking<Attribute> * attributes = nullptr;
    UInt64 index = 0;   /// 1-based index among the element siblings.
    bool is_root = false;

    [[nodiscard]] const Attribute * findAttribute(std::string_view lowercase_name) const;
};

bool matchCompound(const CompoundSelector & compound, const ElementInfo & element);


class Selector
{
public:
    static constexpr size_t max_steps = 64;

    /// Throws an exception if the selector is invalid or unsupported.
    explicit Selector(std::string_view text);

    struct Step
    {
        CompoundSelector compound;
        Combinator combinator = Combinator::None;
        size_t selector_index = 0;      /// Index of the complex selector in the selector list.
    };

    struct ComplexSelectorOutput
    {
        Output output = Output::OuterHTML;
        String attribute;               /// For ::attr(name), in lowercase.
    };

    VectorWithMemoryTracking<Step> steps;
    VectorWithMemoryTracking<ComplexSelectorOutput> outputs;

    /// Bit k is set, if step k has this property.
    UInt64 first_steps = 0;
    UInt64 descendant_steps = 0;
    UInt64 child_steps = 0;
    UInt64 next_sibling_steps = 0;
    UInt64 subsequent_sibling_steps = 0;
    UInt64 last_steps = 0;
    UInt64 steps_needing_attributes = 0;
};


/// A match of a selector in a document. All offsets are relative to the beginning of the document.
struct Match
{
    size_t begin = 0;               /// Beginning of the start tag.
    size_t content_begin = 0;       /// End of the start tag.
    size_t content_end = 0;         /// Beginning of the end tag (or the position where the element was implicitly closed).
    size_t end = 0;                 /// End of the end tag.
    std::string_view attribute;     /// For Output::Attribute.
    Output output = Output::OuterHTML;
    bool closed = false;
    bool raw_text = false;          /// The content of the element is not HTML: `script`, `style`, ...
    bool escapable_raw_text = false; /// `textarea`, `title`.
};


/// Finds the matches of a selector in a document. Reusable for many documents to avoid allocations.
class Matcher
{
public:
    explicit Matcher(const Selector & selector_) : selector(selector_) {}

    /// Returns the matches in the document order.
    /// If `first_only`, returns at most one match and stops scanning as soon as it is complete.
    const VectorWithMemoryTracking<Match> & find(std::string_view document, bool first_only);

private:
    struct OpenElement
    {
        std::string_view name;
        UInt32 kind = 0;
        UInt64 matched = 0;                 /// Steps matched by this element.
        UInt64 matched_by_ancestors = 0;    /// Steps matched by this element or its ancestors.
        UInt64 matched_by_previous_sibling = 0;  /// Of the last element child of this element.
        UInt64 matched_by_previous_siblings = 0; /// Of all element children of this element.
        UInt64 children = 0;
        ssize_t match_index = -1;
    };

    const Selector & selector;
    VectorWithMemoryTracking<OpenElement> stack;
    VectorWithMemoryTracking<Attribute> attributes;
    VectorWithMemoryTracking<Match> matches;
    bool first_only = false;

    /// Returns true if scanning should stop.
    /// The steps that the next child of the current element may match, based on its context.
    [[nodiscard]] UInt64 candidateSteps() const;
    bool openElement(std::string_view name, UInt32 kind, size_t begin, size_t content_begin, UInt64 candidates);
    bool closeElement(size_t content_end, size_t end);
    void closeImplicitly(UInt32 targets, UInt32 boundaries, size_t pos, bool & stop);
    [[nodiscard]] bool isComplete() const;
};

}
