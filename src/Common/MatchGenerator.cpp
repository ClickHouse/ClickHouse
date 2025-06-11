#include <Common/re2.h>

#ifdef LOG_INFO
#undef LOG_INFO
#undef LOG_WARNING
#undef LOG_ERROR
#undef LOG_FATAL
#endif

#include "MatchGenerator.h"

#include <base/EnumReflection.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

#include <map>
#include <functional>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
}


namespace re2
{

class RandomStringPrepareWalker : public Regexp::Walker<Regexp *>
{
private:
    static constexpr int ImplicitMax = 100;

    using Children = std::vector<Regexp *>;

    class Generators;

    /// This function objects look much prettier than lambda expression when stack traces are printed
    class NodeFunction
    {
    public:
        virtual size_t operator() (char * out, size_t size) = 0;
        virtual size_t getRequiredSize() = 0;
        virtual ~NodeFunction() = default;
    };

    using NodeFunctionPtr = std::shared_ptr<NodeFunction>;
    using NodeFuncs = std::vector<NodeFunctionPtr>;

    static NodeFuncs getFuncs(const Children & children_, const Generators & generators_)
    {
        NodeFuncs result;
        result.reserve(children_.size());

        for (auto * child: children_)
        {
            result.push_back(generators_.at(child));
        }

        return result;
    }

    class Generators: public std::map<re2::Regexp *, NodeFunctionPtr> {};

    class RegexpConcatFunction : public NodeFunction
    {
    public:
        RegexpConcatFunction(const Children & children_, const Generators & generators_)
            : children(getFuncs(children_, generators_))
        {
        }

        size_t operator () (char * out, size_t size) override
        {
            size_t total_size = 0;

            for (auto & child: children)
            {
                size_t consumed = child->operator()(out, size);
                chassert(consumed <= size);
                out += consumed;
                size -= consumed;
                total_size += consumed;
            }

            return total_size;
        }

        size_t getRequiredSize() override
        {
            size_t total_size = 0;
            for (auto & child: children)
                total_size += child->getRequiredSize();
            return total_size;
        }

    private:
        NodeFuncs children;
    };

    class RegexpAlternateFunction : public NodeFunction
    {
    public:
        RegexpAlternateFunction(const Children & children_, const Generators & generators_)
            : children(getFuncs(children_, generators_))
        {
        }

        size_t operator () (char * out, size_t size) override
        {
            std::uniform_int_distribution<int> distribution(0, static_cast<int>(children.size()-1));
            int chosen = distribution(thread_local_rng);
            size_t consumed = children[chosen]->operator()(out, size);
            chassert(consumed <= size);
            return consumed;
        }

        size_t getRequiredSize() override
        {
            size_t total_size = 0;
            for (auto & child: children)
                total_size = std::max(total_size, child->getRequiredSize());
            return total_size;
        }

    private:
        NodeFuncs children;
    };

    class RegexpRepeatFunction : public NodeFunction
    {
    public:
        RegexpRepeatFunction(Regexp * re_, const Generators & generators_, int min_repeat_, int max_repeat_)
            : func(generators_.at(re_))
            , min_repeat(min_repeat_)
            , max_repeat(max_repeat_)
        {
        }

        size_t operator () (char * out, size_t size) override
        {
            std::uniform_int_distribution<int> distribution(min_repeat, max_repeat);
            int ntimes = distribution(thread_local_rng);

            size_t total_size = 0;
            for (int i = 0; i < ntimes; ++i)
            {
                size_t consumed =func->operator()(out, size);
                chassert(consumed <= size);
                out += consumed;
                size -= consumed;
                total_size += consumed;
            }
            return total_size;
        }

        size_t getRequiredSize() override
        {
            return max_repeat * func->getRequiredSize();
        }

    private:
        NodeFunctionPtr func;
        int min_repeat = 0;
        int max_repeat = 0;
    };

    class RegexpCharClassFunction : public NodeFunction
    {
        using CharRanges = std::vector<std::pair<re2::Rune, re2::Rune>>;

    public:
        explicit RegexpCharClassFunction(Regexp * re_)
        {
            CharClass * cc = re_->cc();
            chassert(cc);
            if (cc->empty())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "kRegexpCharClass is empty");

            char_count = cc->size();
            char_ranges.reserve(std::distance(cc->begin(), cc->end()));

            for (const auto range: *cc)
            {
                char_ranges.emplace_back(range.lo, range.hi);
            }
        }

        size_t operator () (char * out, size_t size) override
        {
            chassert(UTFmax <= size);

            std::uniform_int_distribution<int> distribution(1, char_count);
            int chosen = distribution(thread_local_rng);
            int count_down = chosen;

            auto it = char_ranges.begin();
            for (; it != char_ranges.end(); ++it)
            {
                auto [lo, hi] = *it;
                auto range_len = hi - lo + 1;
                if (count_down <= range_len)
                    break;
                count_down -= range_len;
            }

            if (it == char_ranges.end())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                                    "Unable to choose the rune. Runes {}, ranges {}, chosen {}",
                                    char_count, char_ranges.size(), chosen);

            auto [lo, _] = *it;
            Rune r = lo + count_down - 1;
            return re2::runetochar(out, &r);
        }

        size_t getRequiredSize() override
        {
            return UTFmax;
        }

    private:
        int char_count = 0;
        CharRanges char_ranges;
    };

    class RegexpLiteralStringFunction : public NodeFunction
    {
    public:
        explicit RegexpLiteralStringFunction(Regexp * re_)
        {
            if (re_->nrunes() == 0)
                return;

            char buffer[UTFmax];
            for (int i = 0; i < re_->nrunes(); ++i)
            {
                int n = re2::runetochar(buffer, &re_->runes()[i]);
                literal_string += String(buffer, n);
            }
        }

        size_t operator () (char * out, size_t size) override
        {
            chassert(literal_string.size() <= size);

            memcpy(out, literal_string.data(), literal_string.size());
            return literal_string.size();
        }

        size_t getRequiredSize() override
        {
            return literal_string.size();
        }

    private:
        String literal_string;
    };

    class RegexpLiteralFunction : public NodeFunction
    {
    public:
        explicit RegexpLiteralFunction(Regexp * re_)
        {
            char buffer[UTFmax];

            Rune r = re_->rune();
            int n = re2::runetochar(buffer, &r);
            literal = String(buffer, n);
        }

        size_t operator () (char * out, size_t size) override
        {
            chassert(literal.size() <= size);

            memcpy(out, literal.data(), literal.size());
            return literal.size();
        }

        size_t getRequiredSize() override
        {
            return literal.size();
        }

    private:
        String literal;
    };

    class ThrowExceptionFunction : public NodeFunction
    {
    public:
        explicit ThrowExceptionFunction(Regexp * re_)
            : operation(magic_enum::enum_name(re_->op()))
        {
        }

        size_t operator () (char *, size_t) override
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "RandomStringPrepareWalker: regexp node '{}' is not supported for generating a random match",
                operation);
        }

        size_t getRequiredSize() override
        {
            return 0;
        }

    private:
        String operation;
    };


public:
    std::function<String()> getGenerator()
    {
        if (root == nullptr)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "no root has been set");

        if (generators.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "no generators");

        auto root_func = generators.at(root);
        auto required_buffer_size = root_func->getRequiredSize();
        auto generator_func = [=] ()
            -> String
        {
            auto buffer = String(required_buffer_size, '\0');
            size_t size = root_func->operator()(buffer.data(), buffer.size());
            buffer.resize(size);
            return buffer;
        };

        root = nullptr;
        generators = {};

        return std::move(generator_func);
    }

private:
    Children CopyChildrenArgs(Regexp ** children, int nchild)
    {
        Children result;
        result.reserve(nchild);
        for (int i = 0; i < nchild; ++i)
            result.push_back(Copy(children[i]));
        return result;
    }

    Regexp * ShortVisit(Regexp* /*re*/, Regexp * /*parent_arg*/) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ShortVisit should not be called");
    }

    Regexp * PreVisit(Regexp * re, Regexp * parent_arg, bool* /*stop*/) override /*noexcept*/
    {
        if (parent_arg == nullptr)
        {
            chassert(root == nullptr);
            chassert(re != nullptr);
            root = re;
        }

        return re;
    }

    Regexp * PostVisit(Regexp * re, Regexp * /*parent_arg*/, Regexp * pre_arg,
                       Regexp ** child_args, int nchild_args) override /*noexcept*/
    {
        switch (re->op())
        {
            case kRegexpConcat: // Matches concatenation of sub_[0..nsub-1].
                generators[re] = std::make_shared<RegexpConcatFunction>(CopyChildrenArgs(child_args, nchild_args), generators);
                break;
            case kRegexpAlternate: // Matches union of sub_[0..nsub-1].
                generators[re] = std::make_shared<RegexpAlternateFunction>(CopyChildrenArgs(child_args, nchild_args), generators);
                break;
            case kRegexpQuest: // Matches sub_[0] zero or one times.
                chassert(nchild_args == 1);
                generators[re] = std::make_shared<RegexpRepeatFunction>(child_args[0], generators, 0, 1);
                break;
            case kRegexpStar: // Matches sub_[0] zero or more times.
                chassert(nchild_args == 1);
                generators[re] = std::make_shared<RegexpRepeatFunction>(child_args[0], generators, 0, ImplicitMax);
                break;
            case kRegexpPlus: // Matches sub_[0] one or more times.
                chassert(nchild_args == 1);
                generators[re] = std::make_shared<RegexpRepeatFunction>(child_args[0], generators, 1, ImplicitMax);
                break;
            case kRegexpCharClass: // Matches character class given by cc_.
                chassert(nchild_args == 0);
                generators[re] = std::make_shared<RegexpCharClassFunction>(re);
                break;
            case kRegexpLiteralString: // Matches runes_.
                chassert(nchild_args == 0);
                generators[re] = std::make_shared<RegexpLiteralStringFunction>(re);
                break;
            case kRegexpLiteral: // Matches rune_.
                chassert(nchild_args == 0);
                generators[re] = std::make_shared<RegexpLiteralFunction>(re);
                break;
            case kRegexpCapture: // Parenthesized (capturing) subexpression.
                chassert(nchild_args == 1);
                generators[re] = generators.at(child_args[0]);
                break;

            case kRegexpNoMatch: // Matches no strings.
            case kRegexpEmptyMatch: // Matches empty string.
            case kRegexpRepeat: // Matches sub_[0] at least min_ times, at most max_ times.
            case kRegexpAnyChar: // Matches any character.
            case kRegexpAnyByte: // Matches any byte [sic].
            case kRegexpBeginLine: // Matches empty string at beginning of line.
            case kRegexpEndLine: // Matches empty string at end of line.
            case kRegexpWordBoundary: // Matches word boundary "\b".
            case kRegexpNoWordBoundary: // Matches not-a-word boundary "\B".
            case kRegexpBeginText: // Matches empty string at beginning of text.
            case kRegexpEndText: // Matches empty string at end of text.
            case kRegexpHaveMatch: // Forces match of entire expression
                generators[re] = std::make_shared<ThrowExceptionFunction>(re);
        }

        return pre_arg;
    }

    Regexp * root = nullptr;
    Generators generators;
};

}


namespace DB
{

void RandomStringGeneratorByRegexp::RegexpPtrDeleter::operator() (re2::Regexp * re) const noexcept
{
    re->Decref();
}

RandomStringGeneratorByRegexp::RandomStringGeneratorByRegexp(const String & re_str)
{
    re2::RE2::Options options;
    options.set_case_sensitive(true);
    options.set_encoding(re2::RE2::Options::EncodingLatin1);
    auto flags = static_cast<re2::Regexp::ParseFlags>(options.ParseFlags());

    re2::RegexpStatus status;
    regexp.reset(re2::Regexp::Parse(re_str, flags, &status));

    if (!regexp)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                            "Error parsing regexp '{}': {}",
                            re_str, status.Text());

    regexp.reset(regexp->Simplify());

    auto walker = re2::RandomStringPrepareWalker();
    walker.Walk(regexp.get(), {});
    generatorFunc = walker.getGenerator();

    {
        auto test_check = generate();
        auto matched = RE2::FullMatch(test_check, re2::RE2(re_str));
        if (!matched)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                                "Generator is unable to produce random string for regexp '{}': {}",
                                re_str, test_check);
    }
}

String RandomStringGeneratorByRegexp::generate() const
{
    chassert(generatorFunc);
    return generatorFunc();
}

}
