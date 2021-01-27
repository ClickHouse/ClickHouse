#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <utility>
#include <vector>
#include <algorithm>

#if USE_HYPERSCAN
#  include <hs.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{
struct hxCoarseParseImpl
{
private:
    struct spanInfo
    {
        spanInfo(): id(0), matchSpace(std::pair<unsigned long long, unsigned long long>(0, 0)) {}
        spanInfo(unsigned int matchId, std::pair<unsigned long long, unsigned long long> matchSpan): id(matchId), matchSpace(matchSpan){}
        spanInfo(const spanInfo& obj)
        {
            id = obj.id;
            matchSpace = obj.matchSpace;
        }
        spanInfo& operator=(const spanInfo& obj)
        {
            id = obj.id;
            matchSpace = obj.matchSpace;
            return (*this);
        }

        unsigned int id;
        std::pair<unsigned long long, unsigned long long> matchSpace;
    };
    typedef std::vector<spanInfo> spanElement;
    struct span
    {
        span(): set_script(false), set_style(false), set_semi(false), is_finding_cdata(false) {}

        spanElement copy_stack;         // copy area
        spanElement tag_stack;          // regexp area
        spanInfo script_ptr;            // script pointer
        bool set_script;                // whether set script
        spanInfo style_ptr;             // style pointer
        bool set_style;                 // whether set style
        spanInfo semi_ptr;              // tag ptr
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
        if(current_dst_string_offset == 0 || dst_chars[current_dst_string_offset - 1] == 0 || dst_chars[current_dst_string_offset - 1] == ' ')
        {
            is_last_space = true;
        }
        if(bytes_to_copy == 0)
        {
            if(is_space && !is_last_space)
            {
                dst_chars[current_dst_string_offset++] = ' ';
            }
        }
        else
        {
            if(is_last_space && src_chars[current_copy_loc] == ' ')
            {
                --bytes_to_copy;
                ++current_copy_loc;
            }
            if(bytes_to_copy > 0)
            {
                memcpySmallAllowReadWriteOverflow15(
                    &dst_chars[current_dst_string_offset], &src_chars[current_copy_loc], bytes_to_copy);
                current_dst_string_offset += bytes_to_copy;
            }

            // seperator is space and last character is not space.
            if(is_space && !(current_dst_string_offset == 0 || dst_chars[current_dst_string_offset - 1] == 0 || dst_chars[current_dst_string_offset - 1] == ' '))
            {
                dst_chars[current_dst_string_offset++] = ' ';
            }
        }
        return;
    }
    static inline void popArea(spanElement& stack, unsigned long long from, unsigned long long to)
    {
        while(!stack.empty())
        {
            if(to > stack.back().matchSpace.second && from < stack.back().matchSpace.second)
            {
                stack.pop_back();
            }
            else
            {
                break;
            }
        }
        return;
    }

    static void deal_common_tag(span* matches)
    {
        while(!matches->copy_stack.empty() && matches->copy_stack.back().id != 10)
        {
            matches->copy_stack.pop_back();
        }
        if(!matches->copy_stack.empty())
        {
            matches->copy_stack.pop_back();
        }
        unsigned long long from;
        unsigned long long to;
        unsigned id;
        for(auto begin = matches->tag_stack.begin(); begin != matches->tag_stack.end(); ++begin)
        {
            from = begin->matchSpace.first;
            to = begin->matchSpace.second;
            id = begin->id;
            switch(id)
            {
                case 12:
                case 13:
                {
                    popArea(matches->copy_stack, from, to);
                    if(matches->copy_stack.empty() || from >= matches->copy_stack.back().matchSpace.second)
                        matches->copy_stack.push_back(spanInfo(id, std::make_pair(from, to)));
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
                    if(!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.matchSpace.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = spanInfo(id, std::make_pair(from, to));
                    }
                    break;
                }
                case 1:
                {
                    // if(!matches->copy_stack.empty() && matches->copy_stack.back().id == 11 && to == matches->copy_stack.back().matchSpace.second)
                    if(matches->set_semi)
                    {
                        switch(matches->semi_ptr.id)
                        {
                            case 0:
                            case 2:
                            case 3:
                            case 6:
                            case 7:
                            case 10:
                            {
                                if(matches->semi_ptr.id == 2 || (matches->semi_ptr.id == 3 && matches->semi_ptr.matchSpace.second == from))
                                {
                                    if(!matches->set_script)
                                    {
                                        matches->set_script = true;
                                        matches->script_ptr = spanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.matchSpace.first, to));
                                    }
                                }
                                else if (matches->semi_ptr.id == 6 || (matches->semi_ptr.id == 7 && matches->semi_ptr.matchSpace.second == from))
                                {
                                    if(!matches->set_style)
                                    {
                                        matches->set_style = true;
                                        matches->style_ptr = spanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.matchSpace.first, to));
                                    }
                                }
                                popArea(matches->copy_stack, matches->semi_ptr.matchSpace.first, to);
                                // if(matches->copy_stack.empty() || matches->semi_ptr.matchSpace.first >= matches->copy_stack.back().matchSpace.second)
                                matches->copy_stack.push_back(spanInfo(0, std::make_pair(matches->semi_ptr.matchSpace.first, to)));
                                matches->set_semi = false;
                                break;
                            }
                            case 4:
                            case 5:
                            case 8:
                            case 9:
                            {
                                spanInfo completeZone;

                                completeZone.matchSpace.second = to;
                                if(matches->set_script && (matches->semi_ptr.id == 4 || (matches->semi_ptr.id == 5 && matches->semi_ptr.matchSpace.second == from)))
                                {
                                    completeZone.id = matches->script_ptr.id;
                                    completeZone.matchSpace.first = matches->script_ptr.matchSpace.first;
                                    matches->set_script = false;
                                }
                                else if(matches->set_style && (matches->semi_ptr.id == 8 || (matches->semi_ptr.id == 9 && matches->semi_ptr.matchSpace.second == from)))
                                {
                                    completeZone.id = matches->style_ptr.id;
                                    completeZone.matchSpace.first = matches->style_ptr.matchSpace.first;
                                    matches->set_style = false;
                                }
                                else
                                {
                                    completeZone.id = matches->semi_ptr.id;
                                    completeZone.matchSpace.first = matches->semi_ptr.matchSpace.first;
                                }
                                // if(matches->copy_stack.empty() || completeZone.matchSpace.first >= matches->copy_stack.back().matchSpace.second)
                                popArea(matches->copy_stack, completeZone.matchSpace.first, completeZone.matchSpace.second);
                                matches->copy_stack.push_back(completeZone);
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
        return;
    }
    static int spanCollect(unsigned int id,
                          unsigned long long from,
                          unsigned long long to,
                          unsigned int , void * ctx)
    {
        span* matches = static_cast<span*>(ctx);
        from = id == 12 ? from : to - patterns_length[id];

        if(matches->is_finding_cdata)
        {
            if(id == 11)
            {
                matches->copy_stack.push_back(spanInfo(id, std::make_pair(from, to)));
                matches->is_finding_cdata = false;
                matches->tag_stack.clear();
                if(matches->semi_ptr.id == 10)
                {
                    matches->set_semi = false;
                }
            }
            else if(id == 12 || id == 13)
            {
                popArea(matches->copy_stack, from, to);
                if(matches->copy_stack.empty() || from >= matches->copy_stack.back().matchSpace.second)
                    matches->copy_stack.push_back(spanInfo(id, std::make_pair(from, to)));

                popArea(matches->tag_stack, from, to);
                if(matches->tag_stack.empty() || from >= matches->tag_stack.back().matchSpace.second)
                    matches->tag_stack.push_back(spanInfo(id, std::make_pair(from, to)));
            }
            else
            {
                popArea(matches->tag_stack, from, to);
                // if(matches->tag_stack.empty() || from >= matches->tag_stack.back().matchSpace.second)
                matches->tag_stack.push_back(spanInfo(id, std::make_pair(from, to)));
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
                    if(matches->copy_stack.empty() || from >= matches->copy_stack.back().matchSpace.second)
                        matches->copy_stack.push_back(spanInfo(id, std::make_pair(from, to)));
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
                    if(!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.matchSpace.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = spanInfo(id, std::make_pair(from, to));
                    }
                    break;
                }
                case 10:
                {
                    if(!matches->set_semi || (matches->set_semi && from == matches->semi_ptr.matchSpace.first))
                    {
                        matches->set_semi = true;
                        matches->semi_ptr = spanInfo(id, std::make_pair(from, to));
                    }
                    matches->is_finding_cdata = true;
                    matches->copy_stack.push_back(spanInfo(id, std::make_pair(from, to)));
                    matches->tag_stack.push_back(spanInfo(id, std::make_pair(from, to)));
                    break;
                }
                case 1:
                {
                    // if(!matches->copy_stack.empty() && matches->copy_stack.back().id == 11 && to == matches->copy_stack.back().matchSpace.second)
                    if(matches->set_semi)
                    {
                        switch(matches->semi_ptr.id)
                        {
                            case 0:
                            case 2:
                            case 3:
                            case 6:
                            case 7:
                            case 10:
                            {
                                if(matches->semi_ptr.id == 2 || (matches->semi_ptr.id == 3 && matches->semi_ptr.matchSpace.second == from))
                                {
                                    if(!matches->set_script)
                                    {
                                        matches->set_script = true;
                                        matches->script_ptr = spanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.matchSpace.first, to));
                                    }
                                }
                                else if (matches->semi_ptr.id == 6 || (matches->semi_ptr.id == 7 && matches->semi_ptr.matchSpace.second == from))
                                {
                                    if(!matches->set_style)
                                    {
                                        matches->set_style = true;
                                        matches->style_ptr = spanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.matchSpace.first, to));
                                    }
                                }
                                popArea(matches->copy_stack, matches->semi_ptr.matchSpace.first, to);
                                // if(matches->copy_stack.empty() || matches->semi_ptr.matchSpace.first >= matches->copy_stack.back().matchSpace.second)
                                matches->copy_stack.push_back(spanInfo(matches->semi_ptr.id, std::make_pair(matches->semi_ptr.matchSpace.first, to)));
                                matches->set_semi = false;
                                break;
                            }
                            case 4:
                            case 5:
                            case 8:
                            case 9:
                            {
                                spanInfo completeZone;
                                completeZone.matchSpace.second = to;
                                if(matches->set_script && (matches->semi_ptr.id == 4 || (matches->semi_ptr.id == 5 && matches->semi_ptr.matchSpace.second == from)))
                                {
                                    completeZone.id = matches->script_ptr.id;
                                    completeZone.matchSpace.first = matches->script_ptr.matchSpace.first;
                                    matches->set_script = false;
                                }
                                else if(matches->set_style && (matches->semi_ptr.id == 8 || (matches->semi_ptr.id == 9 && matches->semi_ptr.matchSpace.second == from)))
                                {
                                    completeZone.id = matches->style_ptr.id;
                                    completeZone.matchSpace.first = matches->style_ptr.matchSpace.first;
                                    matches->set_style = false;
                                }
                                else
                                {
                                    completeZone.id = matches->semi_ptr.id;
                                    completeZone.matchSpace.first = matches->semi_ptr.matchSpace.first;
                                }
                                // if(matches->copy_stack.empty() || completeZone.matchSpace.first >= matches->copy_stack.back().matchSpace.second)
                                popArea(matches->copy_stack, completeZone.matchSpace.first, completeZone.matchSpace.second);
                                matches->copy_stack.push_back(completeZone);
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
                                        const std::vector<const unsigned> flags,
                                        const std::vector<const unsigned> id,
                                        unsigned int mode)
    {
        hs_database_t *db;
        hs_compile_error_t *compileErr;
        hs_error_t err;
        err = hs_compile_multi(expressions.data(), flags.data(), id.data(),
                            expressions.size(), mode, nullptr, &db, &compileErr);

        if (err != HS_SUCCESS)
        {
            hs_free_compile_error(compileErr);
            throw Exception("Hyper scan database cannot be compiled.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        return db;
    }
    #endif
    static std::vector<const char*> patterns;
    static std::vector<const std::size_t> patterns_length;
    static std::vector<const unsigned> patterns_flag;
    static std::vector<const unsigned> ids;

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
            if(hs_alloc_scratch(db, &scratch) != HS_SUCCESS)
            {
                hs_free_database(db);
                throw Exception("Unable to allocate scratch space.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
            }
        #else
            throw Exception("The function must depend on the third party: HyperScan. Please check your compile option.", ErrorCodes::CANNOT_COMPILE_REGEXP);
        #endif
        dst_chars.resize(src_chars.size());
        dst_offsets.resize(src_offsets.size());

        ColumnString::Offset current_src_string_offset = 0;
        ColumnString::Offset current_dst_string_offset = 0;
        ColumnString::Offset current_copy_loc;
        ColumnString::Offset current_copy_end;
        unsigned is_space;
        size_t bytes_to_copy;
        span matchZoneAll;

        for(size_t off = 0; off < src_offsets.size(); ++off)
        {
            #if USE_HYPERSCAN
                hs_scan(db, reinterpret_cast<const char *>(&src_chars[current_src_string_offset]), src_offsets[off] - current_src_string_offset, 0, scratch, spanCollect, &matchZoneAll);
            #else
                throw Exception("The function must depend on the third party: HyperScan. Please check your compile option.", ErrorCodes::ILLEGAL_COLUMN);
            #endif
            for(size_t i = 0; i < matchZoneAll.tag_stack.size(); ++i)
            {
                std::cout << matchZoneAll.tag_stack[i].id << " " << matchZoneAll.tag_stack[i].matchSpace.first << " " << matchZoneAll.tag_stack[i].matchSpace.second << std::endl;
            }
            if(matchZoneAll.is_finding_cdata)
            {
                deal_common_tag(&matchZoneAll);
            }
            spanElement& matchZone = matchZoneAll.copy_stack;
            current_copy_loc = current_src_string_offset;
            if(matchZone.empty())
            {
                current_copy_end = src_offsets[off];
                is_space = 0;
            }
            else
            {
                current_copy_end = current_src_string_offset + matchZone.begin()->matchSpace.first;
                is_space = (matchZone.begin()->id == 12 || matchZone.begin()->id == 13)?1:0;
            }

            bytes_to_copy = current_copy_end - current_copy_loc;
            copyZone(current_dst_string_offset, current_copy_loc, dst_chars, src_chars, bytes_to_copy, is_space);
            for(auto begin = matchZone.begin(); begin != matchZone.end(); ++begin)
            {
                current_copy_loc = current_src_string_offset + begin->matchSpace.second;
                if(begin + 1 >= matchZone.end())
                {
                    current_copy_end = src_offsets[off];
                    is_space = 0;
                }
                else
                {
                    current_copy_end = current_src_string_offset + (begin+1)->matchSpace.first;
                    is_space = ((begin+1)->id == 12 || (begin+1)->id == 13)?1:0;
                }
                bytes_to_copy = current_copy_end - current_copy_loc;
                copyZone(current_dst_string_offset, current_copy_loc, dst_chars, src_chars, bytes_to_copy, is_space);
            }
            if(current_dst_string_offset > 1 && dst_chars[current_dst_string_offset - 2] == ' ')
            {
                dst_chars[current_dst_string_offset - 2] = 0;
                --current_dst_string_offset;
            }
            dst_offsets[off] = current_dst_string_offset;
            current_src_string_offset = src_offsets[off];
            matchZoneAll.copy_stack.clear();
            matchZoneAll.tag_stack.clear();
        }
        #if USE_HYPERSCAN
            dst_chars.resize(dst_chars.size());
            hs_free_scratch(scratch);
            hs_free_database(db);
        #endif
    }
};

std::vector<const char*> hxCoarseParseImpl::patterns =
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
        "\\s{2,}",         // 12 "   ", continous blanks
        "[^\\S ]"          // 13 "\n", "\t" and other white space, it does not include single ' '.
    };
std::vector<const std::size_t> hxCoarseParseImpl::patterns_length =
    {
        2, 1, 8, 7, 9, 8, 7, 6, 8, 7, 9, 3, 0, 1
    };
#if USE_HYPERSCAN
std::vector<const unsigned> hxCoarseParseImpl::patterns_flag =
    {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, HS_FLAG_SOM_LEFTMOST, 0
    };
#endif

std::vector<const unsigned> hxCoarseParseImpl::ids =
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
        if(!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override {return true;}

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t) const override
    {
        const auto & strcolumn = arguments[0].column;
        if(const ColumnString* htmlSentence = checkAndGetColumn<ColumnString>(strcolumn.get()))
        {
            auto col_res = ColumnString::create();
            hxCoarseParseImpl::executeInternal(htmlSentence->getChars(), htmlSentence->getOffsets(), col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else
        {
            throw Exception("First argument for function " + getName() + " must be string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENTS);
        }
    }
};
}

void registerFunctionHtmlOrXmlCoarseParse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHtmlOrXmlCoarseParse>();
}

}
