#include <Functions/JSONPath/Generators/IGenerator.h>
#include <Functions/JSONPath/Generators/VisitorJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generators/VisitorJSONPathRange.h>
#include <Functions/JSONPath/Generators/VisitorStatus.h>

#include <Functions/JSONPath/ASTs/ASTJSONPath.h>


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

        for (auto child_ast : query->children)
        {
            if (child_ast->getID() == "ASTJSONPathMemberAccess")
            {
                auto member_access_visitor = std::make_shared<VisitorJSONPathMemberAccess<JSONParser>>(child_ast);
                if (member_access_visitor) {
                    visitors.push_back(member_access_visitor);
                } else {
                    throw Exception("member_access_visitor could not be nullptr", ErrorCodes::LOGICAL_ERROR);
                }
            } else if (child_ast->getID() == "ASTJSONPathRange") {
                auto range_visitor = std::make_shared<VisitorJSONPathRange<JSONParser>>(child_ast);
                if (range_visitor) {
                    visitors.push_back(range_visitor);
                } else {
                    throw Exception("range_visitor could not be nullptr", ErrorCodes::LOGICAL_ERROR);
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
        while (true) {
            auto root = element;
            if (current_visitor < 0) {
                return VisitorStatus::Exhausted;
            }

            for (int i = 0; i < current_visitor; ++i) {
                visitors[i]->apply(root);
            }

            VisitorStatus status = VisitorStatus::Error;
            for (size_t i = current_visitor; i < visitors.size(); ++i) {
                status = visitors[i]->visit(root);
                current_visitor = i;
                if (status == VisitorStatus::Error || status == VisitorStatus::Ignore) {
                    break;
                }
            }
            updateVisitorsForNextRun();

            if (status != VisitorStatus::Ignore) {
                element = root;
                return status;
            }
        }
    }

private:
    bool updateVisitorsForNextRun() {
        while (current_visitor >= 0 && visitors[current_visitor]->isExhausted()) {
            visitors[current_visitor]->reinitialize();
            current_visitor--;
        }
        if (current_visitor >= 0) {
            visitors[current_visitor]->updateState();
        }
        return current_visitor >= 0;
    }

    int current_visitor = 0;
    ASTPtr query_ptr;
    VisitorList<JSONParser> visitors;
};

} // namespace DB
