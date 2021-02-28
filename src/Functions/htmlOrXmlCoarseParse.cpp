#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <utility>
#include <vector>
#include <algorithm>

#if USE_HYPERSCAN
#   include <hs.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
struct HxCoarseParseImpl
{
private:
    struct SpanInfo
    {
        SpanInfo(): id(0), match_space(std::pair<unsigned long long, unsigned long long>(0, 0)) {}  // NOLINT
        SpanInfo(unsigned int matchId, std::pair<unsigned long long, unsigned long long> matchSpan): id(matchId), match_space(matchSpan){} // NOLINT
        SpanInfo(const SpanInfo& obj)
        {
            id = obj.id;
            match_space = obj.match_space;
        }
        SpanInfo& operator=(const SpanInfo& obj) = default;

        unsigned int id;
        std::pair<unsigned long long, unsigned long long> match_space;  // NOLINT
    };
    using SpanElement = std::vector<SpanInfo>;
    struct Span
    {
        Span(): set_script(false), set_style(false), set_semi(false), is_finding_cdata(false) {}

        SpanElement copy_stack;         // copy area
        SpanElement tag_stack;          // regexp area
        SpanInfo script_ptr;            // script pointer
        bool set_script;                // whether set script
        SpanInfo style_ptr;             // style pointer
        bool set_style;                 // whether set style
        SpanInfo semi_ptr;              // tag ptr
        bool set_semi;                  // whether set semi

        bool is_finding_cdata;
    };

    static inline void copyZone(
        ColumnString::Offset& current_dst_string_offset,
        ColumnString::Offset& current_copy_loc,
        ColumnString::Chars& dst_chars,
        const ColumnString::Chars& src_chars,
        size_t bytes_to_copy,
        unsigned is_space
    )
    {
        bool is_last_space = false;
        if (current_dst_string_offset == 0 || dst_chars[current_dst_string_offset - 1] == 0 || dst_chars[current_dst_string_offset - 1] == ' ')
        {
            is_last_space = true;
        }
        if (bytes_to_copy == 0)
        {
            if (is_space && !is_last_space)
            {
                dst_chars[current_dst_string_offset++] = ' ';
            }
        }
        else
        {
            if (is_last_space && src_chars[current_copy_loc] == ' ')
            {
                --bytes_to_copy;
                ++current_copy_loc;
            }
            if (bytes_to_copy > 0)
            {
                memcpySmallAllowReadWriteOverflow15(
                    &dst_chars[current_dst_string_offset], &src_chars[current_copy_loc], bytes_to_copy);
                current_dst_string_offset += bytes_to_copy;
            }

            // separator is space and last character is not space.
            if (is_space && !(current_dst_string_offset == 0 || dst_chars[current_dst_string_offset - 1] == 0 || dst_chars[current_dst_string_offset - 1] == ' '))
            {
                dst_chars[current_dst_string_offset++] = ' ';
            }
        }
        // return;
    }
    static inline void popArea(SpanElement& stack, unsigned long long from, unsigned long long to)  //NOLINT
    {
        while (!stack.empty())
        {
            if (to > stack.back().match_space.second && from < stack.back().match_space.second)
            {
                stack.pop_back();
            }
            else
            {
                break;
            }
        }
        // return;
    }

    static void dealCommonTag(Span* matches)
    {
        while (!matches->copy_stack.empty() && matches->copy_stack.back().id != 10)
        {
            matches->copy_stack.pop_back();
        }
        if (!matches->copy_stack.empty())
        {
            matches->copy_stack.pop_back();
        }
        unsigned long long from;    // NOLINT
        unsigned long long to;      // NOLINT
        unsigned id;
        for (auto begin = matches->tag_stack.begin(); begin != matches->tag_stack.end(); ++begin)
        {
            from = begin->match_space.first;
            to = begin->match_space.second;
            id = begin->id;
            switch (id)
            {
                case 12:
                case 13:
                {
                    popArea(matches->copy_stack, from, to);
                    if (matches->copy_stack.empty() || from >= matches->copy_stack.back().match_space.second)
                        matches->copy_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
                    break;
                }
                case 0:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                case 8:
                case 9:
                case 10:
                {
                    if (!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.match_space.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = SpanInfo(id, std::make_pair(from, to));
                    }
                    break;
                }
                case 1:
                {
                    if (matches->set_semi)
                    {
                        switch (matches->semi_ptr.id)
                        {
                            case 0:
                            case 2:
                            case 3:
                            case 6:
                            case 7:
                            case 10:
                            {
                                if (matches->semi_ptr.id == 2 || (matches->semi_ptr.id == 3 && matches->semi_ptr.match_space.second == from))
                                {
                                    if (!matches->set_script)
                                    {
                                        matches->set_script = true;
                                        matches->script_ptr = SpanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.match_space.first, to));
                                    }
                                }
                                else if (matches->semi_ptr.id == 6 || (matches->semi_ptr.id == 7 && matches->semi_ptr.match_space.second == from))
                                {
                                    if (!matches->set_style)
                                    {
                                        matches->set_style = true;
                                        matches->style_ptr = SpanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.match_space.first, to));
                                    }
                                }
                                popArea(matches->copy_stack, matches->semi_ptr.match_space.first, to);
                                matches->copy_stack.push_back(SpanInfo(0, std::make_pair(matches->semi_ptr.match_space.first, to)));
                                matches->set_semi = false;
                                break;
                            }
                            case 4:
                            case 5:
                            case 8:
                            case 9:
                            {
                                SpanInfo complete_zone;

                                complete_zone.match_space.second = to;
                                if (matches->set_script && (matches->semi_ptr.id == 4 || (matches->semi_ptr.id == 5 && matches->semi_ptr.match_space.second == from)))
                                {
                                    complete_zone.id = matches->script_ptr.id;
                                    complete_zone.match_space.first = matches->script_ptr.match_space.first;
                                    matches->set_script = false;
                                }
                                else if (matches->set_style && (matches->semi_ptr.id == 8 || (matches->semi_ptr.id == 9 && matches->semi_ptr.match_space.second == from)))
                                {
                                    complete_zone.id = matches->style_ptr.id;
                                    complete_zone.match_space.first = matches->style_ptr.match_space.first;
                                    matches->set_style = false;
                                }
                                else
                                {
                                    complete_zone.id = matches->semi_ptr.id;
                                    complete_zone.match_space.first = matches->semi_ptr.match_space.first;
                                }
                                popArea(matches->copy_stack, complete_zone.match_space.first, complete_zone.match_space.second);
                                matches->copy_stack.push_back(complete_zone);
                                matches->set_semi = false;
                                break;
                            }
                        }
                    }
                    break;
                }
                default:
                {
                    break;
                }
            }
        }
        // return;
    }
    static int spanCollect(unsigned int id,
                          unsigned long long from,  // NOLINT
                          unsigned long long to,    // NOLINT
                          unsigned int , void * ctx)
    {
        Span* matches = static_cast<Span*>(ctx);
        from = id == 12 ? from : to - patterns_length[id];

        if (matches->is_finding_cdata)
        {
            if (id == 11)
            {
                matches->copy_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
                matches->is_finding_cdata = false;
                matches->tag_stack.clear();
                if (matches->semi_ptr.id == 10)
                {
                    matches->set_semi = false;
                }
            }
            else if (id == 12 || id == 13)
            {
                popArea(matches->copy_stack, from, to);
                if (matches->copy_stack.empty() || from >= matches->copy_stack.back().match_space.second)
                    matches->copy_stack.push_back(SpanInfo(id, std::make_pair(from, to)));

                popArea(matches->tag_stack, from, to);
                if (matches->tag_stack.empty() || from >= matches->tag_stack.back().match_space.second)
                    matches->tag_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
            }
            else
            {
                popArea(matches->tag_stack, from, to);
                matches->tag_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
            }
        }
        else
        {
            switch (id)
            {
                case 12:
                case 13:
                {
                    popArea(matches->copy_stack, from, to);
                    if (matches->copy_stack.empty() || from >= matches->copy_stack.back().match_space.second)
                        matches->copy_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
                    break;
                }
                case 0:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                case 8:
                case 9:
                {
                    if (!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.match_space.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = SpanInfo(id, std::make_pair(from, to));
                    }
                    break;
                }
                case 10:
                {
                    if (!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.match_space.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = SpanInfo(id, std::make_pair(from, to));
                    }
                    matches->is_finding_cdata = true;
                    matches->copy_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
                    matches->tag_stack.push_back(SpanInfo(id, std::make_pair(from, to)));
                    break;
                }
                case 1:
                {
                    if (matches->set_semi)
                    {
                        switch (matches->semi_ptr.id)
                        {
                            case 0:
                            case 2:
                            case 3:
                            case 6:
                            case 7:
                            case 10:
                            {
                                if (matches->semi_ptr.id == 2 || (matches->semi_ptr.id == 3 && matches->semi_ptr.match_space.second == from))
                                {
                                    if (!matches->set_script)
                                    {
                                        matches->set_script = true;
                                        matches->script_ptr = SpanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.match_space.first, to));
                                    }
                                }
                                else if (matches->semi_ptr.id == 6 || (matches->semi_ptr.id == 7 && matches->semi_ptr.match_space.second == from))
                                {
                                    if (!matches->set_style)
                                    {
                                        matches->set_style = true;
                                        matches->style_ptr = SpanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.match_space.first, to));
                                    }
                                }
                                popArea(matches->copy_stack, matches->semi_ptr.match_space.first, to);
                                matches->copy_stack.push_back(SpanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.match_space.first, to)));
                                matches->set_semi = false;
                                break;
                            }
                            case 4:
                            case 5:
                            case 8:
                            case 9:
                            {
                                SpanInfo complete_zone;
                                complete_zone.match_space.second = to;
                                if (matches->set_script && (matches->semi_ptr.id == 4 || (matches->semi_ptr.id == 5 && matches->semi_ptr.match_space.second == from)))
                                {
                                    complete_zone.id = matches->script_ptr.id;
                                    complete_zone.match_space.first = matches->script_ptr.match_space.first;
                                    matches->set_script = false;
                                }
                                else if (matches->set_style && (matches->semi_ptr.id == 8 || (matches->semi_ptr.id == 9 && matches->semi_ptr.match_space.second == from)))
                                {
                                    complete_zone.id = matches->style_ptr.id;
                                    complete_zone.match_space.first = matches->style_ptr.match_space.first;
                                    matches->set_style = false;
                                }
                                else
                                {
                                    complete_zone.id = matches->semi_ptr.id;
                                    complete_zone.match_space.first = matches->semi_ptr.match_space.first;
                                }
                                popArea(matches->copy_stack, complete_zone.match_space.first, complete_zone.match_space.second);
                                matches->copy_stack.push_back(complete_zone);
                                matches->set_semi = false;
                                break;
                            }
                        }
                    }
                    break;
                }
                default:
                {
                    break;
                }
            }
        }
        return 0;
    }
    #if USE_HYPERSCAN
    static hs_database_t* buildDatabase(const std::vector<const char* > &expressions,
                                        const std::vector<unsigned> &flags,
                                        const std::vector<unsigned> &id,
                                        unsigned int mode)
    {
        hs_database_t *db;
        hs_compile_error_t *compile_err;
        hs_error_t err;
        err = hs_compile_multi(expressions.data(), flags.data(), id.data(),
                            expressions.size(), mode, nullptr, &db, &compile_err);

        if (err != HS_SUCCESS)
        {
            hs_free_compile_error(compile_err);
            throw Exception("Hyper scan database cannot be compiled.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        return db;
    }
    #endif
    static std::vector<const char*> patterns;
    static std::vector<std::size_t> patterns_length;
    static std::vector<unsigned> patterns_flag;
    static std::vector<unsigned> ids;

public:
    static void executeInternal(
        const ColumnString::Chars & src_chars,
        const ColumnString::Offsets & src_offsets,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_offsets)
    {
    #if USE_HYPERSCAN
        hs_database_t * db = buildDatabase(patterns, patterns_flag, ids, HS_MODE_BLOCK);
        hs_scratch_t* scratch = nullptr;
        if (hs_alloc_scratch(db, &scratch) != HS_SUCCESS)
        {
            hs_free_database(db);
            throw Exception("Unable to allocate scratch space.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        dst_chars.resize(src_chars.size());
        dst_offsets.resize(src_offsets.size());

        ColumnString::Offset current_src_string_offset = 0;
        ColumnString::Offset current_dst_string_offset = 0;
        ColumnString::Offset current_copy_loc;
        ColumnString::Offset current_copy_end;
        unsigned is_space;
        size_t bytes_to_copy;
        Span match_zoneall;

        for (size_t off = 0; off < src_offsets.size(); ++off)
        {
            hs_scan(db, reinterpret_cast<const char *>(&src_chars[current_src_string_offset]), src_offsets[off] - current_src_string_offset, 0, scratch, spanCollect, &match_zoneall);
            if (match_zoneall.is_finding_cdata)
            {
                dealCommonTag(&match_zoneall);
            }
            SpanElement& match_zone = match_zoneall.copy_stack;
            current_copy_loc = current_src_string_offset;
            if (match_zone.empty())
            {
                current_copy_end = src_offsets[off];
                is_space = 0;
            }
            else
            {
                current_copy_end = current_src_string_offset + match_zone.begin()->match_space.first;
                is_space = (match_zone.begin()->id == 12 || match_zone.begin()->id == 13)?1:0;
            }

            bytes_to_copy = current_copy_end - current_copy_loc;
            copyZone(current_dst_string_offset, current_copy_loc, dst_chars, src_chars, bytes_to_copy, is_space);
            for (auto begin = match_zone.begin(); begin != match_zone.end(); ++begin)
            {
                current_copy_loc = current_src_string_offset + begin->match_space.second;
                if (begin + 1 >= match_zone.end())
                {
                    current_copy_end = src_offsets[off];
                    is_space = 0;
                }
                else
                {
                    current_copy_end = current_src_string_offset + (begin+1)->match_space.first;
                    is_space = ((begin+1)->id == 12 || (begin+1)->id == 13)?1:0;
                }
                bytes_to_copy = current_copy_end - current_copy_loc;
                copyZone(current_dst_string_offset, current_copy_loc, dst_chars, src_chars, bytes_to_copy, is_space);
            }
            if (current_dst_string_offset > 1 && dst_chars[current_dst_string_offset - 2] == ' ')
            {
                dst_chars[current_dst_string_offset - 2] = 0;
                --current_dst_string_offset;
            }
            dst_offsets[off] = current_dst_string_offset;
            current_src_string_offset = src_offsets[off];
            match_zoneall.copy_stack.clear();
            match_zoneall.tag_stack.clear();
        }
            dst_chars.resize(dst_chars.size());
            hs_free_scratch(scratch);
            hs_free_database(db);
    #else
        (void)src_chars;
        (void)src_offsets;
        (void)dst_chars;
        (void)dst_offsets;
        throw Exception(
            "htmlOrXmlCoarseParse is not implemented when hyperscan is off (is it x86 processor?)",
            ErrorCodes::NOT_IMPLEMENTED);
    #endif
    }
};

std::vector<const char*> HxCoarseParseImpl::patterns =
    {
        "<[^\\s<>]",       // 0  "<", except "< ", "<<", "<>"
        ">",               // 1  ">"
        "<script\\s",      // 2  <script xxxxx>
        "<script",         // 3  <script>
        "</script\\s",     // 4  </script xxxx>
        "</script",        // 5  </script>
        "<style\\s",       // 6  <style xxxxxx>
        "<style",          // 7  <style>
        "</style\\s",      // 8  </style xxxxx>
        "</style",         // 9  </style>
        "<!\\[CDATA\\[",   // 10 <![CDATA[xxxxxx]]>
        "\\]\\]>",         // 11 ]]>
        "\\s{2,}",         // 12 "   ", continuous blanks
        "[^\\S ]"          // 13 "\n", "\t" and other white space, it does not include single ' '.
    };
std::vector<std::size_t> HxCoarseParseImpl::patterns_length =
    {
        2, 1, 8, 7, 9, 8, 7, 6, 8, 7, 9, 3, 0, 1
    };
#if USE_HYPERSCAN
std::vector<unsigned> HxCoarseParseImpl::patterns_flag =
    {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, HS_FLAG_SOM_LEFTMOST, 0
    };
#endif
std::vector<unsigned> HxCoarseParseImpl::ids =
    {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    };

class FunctionHtmlOrXmlCoarseParse : public IFunction
{
public:
    static constexpr auto name = "htmlOrXmlCoarseParse";

    static FunctionPtr create(const Context &) {return std::make_shared<FunctionHtmlOrXmlCoarseParse>(); }

    String getName() const override {return name;}

    size_t getNumberOfArguments() const override {return 1;}

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override {return true;}

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t) const override
    {
        const auto & strcolumn = arguments[0].column;
        if (const ColumnString* html_sentence = checkAndGetColumn<ColumnString>(strcolumn.get()))
        {
            auto col_res = ColumnString::create();
            HxCoarseParseImpl::executeInternal(html_sentence->getChars(), html_sentence->getOffsets(), col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else
        {
            throw Exception("First argument for function " + getName() + " must be string.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};
}

void registerFunctionHtmlOrXmlCoarseParse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHtmlOrXmlCoarseParse>();
}

}
#endif
