#include <Functions/JSONPath/Generators/IGenerator.h>
#include <Functions/JSONPath/Generators/VisitorJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generators/VisitorStatus.h>

#include <Functions/JSONPath/ASTs/ASTJSONPath.h>

#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename JSONParser>
class GeneratorJSONPath : public IGenerator<JSONParser>
{
public:
    GeneratorJSONPath(ASTPtr query_ptr_)
    {
        query_ptr = query_ptr_;
        const auto * path = query_ptr->as<ASTJSONPath>();
        if (!path) {
            throw Exception("Invalid path", ErrorCodes::LOGICAL_ERROR);
        }
        const auto * query = path->jsonpath_query;
        if (!path || !query)
        {
            throw Exception("Something went terribly wrong", ErrorCodes::LOGICAL_ERROR);
        }

        for (auto child_ast : query->children)
        {
            if (child_ast->getID() == "ASTJSONPathMemberAccess")
            {
                auto member_access_generator = std::make_shared<VisitorJSONPathMemberAccess<JSONParser>>(child_ast);
                if (member_access_generator) {
                    visitors.push_back(member_access_generator);
                } else {
                    throw Exception("member_access_generator could not be nullptr", ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }

    const char * getName() const override { return "GeneratorJSONPath"; }

    /**
     * The only generator which is called from JSONPath functions.
     * @param element root of JSON document
     * @return is the generator exhausted
     */
    VisitorStatus getNextItem(typename JSONParser::Element & element) override
    {
        if (visitors[current_visitor]->isExhausted()) {
            if (!backtrace()) {
                return VisitorStatus::Exhausted;
            }
        }

        /// Apply all non-exhausted visitors
        for (int i = 0; i < current_visitor; ++i) {
            VisitorStatus status = visitors[i]->apply(element);
            /// on fail return immediately
            if (status == VisitorStatus::Error) {
                return status;
            }
        }

        /// Visit newly initialized (for the first time or through reinitialize) visitors
        for (size_t i = current_visitor; i < visitors.size(); ++i) {
            VisitorStatus status = visitors[i]->visit(element);
            current_visitor = i;
            /// on fail return immediately
            if (status == VisitorStatus::Error) {
                return status;
            }
        }
        return VisitorStatus::Ok;
    }

private:
    bool backtrace() {
        while (current_visitor >= 0 && visitors[current_visitor]->isExhausted()) {
            visitors[current_visitor]->reinitialize();
            current_visitor--;
        }
        return current_visitor >= 0;
    }

    int current_visitor = 0;
    ASTPtr query_ptr;
    VisitorList<JSONParser> visitors;
};

} // namespace DB
