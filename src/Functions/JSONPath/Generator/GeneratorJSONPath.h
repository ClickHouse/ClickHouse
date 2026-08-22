#pragma once

#include <Functions/JSONPath/Generator/IGenerator.h>
#include <Functions/JSONPath/Generator/VisitorJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generator/VisitorJSONPathRange.h>
#include <Functions/JSONPath/Generator/VisitorJSONPathRoot.h>
#include <Functions/JSONPath/Generator/VisitorJSONPathStar.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

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
    /**
     * Traverses children ASTs of ASTJSONPathQuery and creates a vector of corresponding visitors
     * @param query_ptr_ pointer to ASTJSONPathQuery
     */
    explicit GeneratorJSONPath(ASTPtr query_ptr_)
    {
        query_ptr = query_ptr_;
        const auto * path = query_ptr->as<ASTJSONPath>();
        if (!path)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid path");
        }
        const auto * query = path->jsonpath_query;

        for (const auto & child_ast : query->children)
        {
            if (typeid_cast<ASTJSONPathRoot *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<VisitorJSONPathRoot<JSONParser>>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathMemberAccess *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<VisitorJSONPathMemberAccess<JSONParser>>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathRange *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<VisitorJSONPathRange<JSONParser>>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathStar *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<VisitorJSONPathStar<JSONParser>>(child_ast));
            }
        }
    }

    const char * getName() const override { return "GeneratorJSONPath"; }

    /**
     * This method exposes API of traversing all paths, described by JSONPath,
     *  to SQLJSON Functions.
     * Expected usage is to iteratively call this method from inside the function
     *  and to execute custom logic with received element or handle an error.
     * On each such call getNextItem will yield next item into element argument
     *  and modify its internal state to prepare for next call.
     *
     * @param element root of JSON document
     * @return is the generator exhausted
     */
    VisitorStatus getNextItem(typename JSONParser::Element & element) override
    {
        while (true)
        {
            /// element passed to us actually is root, so here we assign current to root
            auto current = element;
            if (current_visitor < 0)
            {
                return VisitorStatus::Exhausted;
            }

            for (int i = 0; i < current_visitor; ++i)
            {
                visitors[i]->apply(current);
            }

            VisitorStatus status = VisitorStatus::Error;
            for (size_t i = current_visitor; i < visitors.size(); ++i)
            {
                status = visitors[i]->visit(current);
                current_visitor = static_cast<int>(i);
                if (status == VisitorStatus::Error || status == VisitorStatus::Ignore)
                {
                    break;
                }
            }
            updateVisitorsForNextRun();

            if (status != VisitorStatus::Ignore)
            {
                element = current;
                return status;
            }
        }
    }

    void reinitialize()
    {
        while (current_visitor >= 0)
        {
            visitors[current_visitor]->reinitialize();
            current_visitor--;
        }
        current_visitor = 0;
    }

private:
    bool updateVisitorsForNextRun()
    {
        while (current_visitor >= 0 && visitors[current_visitor]->isExhausted())
        {
            visitors[current_visitor]->reinitialize();
            current_visitor--;
        }
        if (current_visitor >= 0)
        {
            visitors[current_visitor]->updateState();
        }
        return current_visitor >= 0;
    }

    int current_visitor = 0;
    ASTPtr query_ptr;
    VisitorList<JSONParser> visitors;
};

}
