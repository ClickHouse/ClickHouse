#pragma once
#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

/// Additional information for ASTSelectQuery
class SemanticSelectQuery : public ISemantic
{
public:
    SemanticPtr clone() const override { return std::make_shared<SemanticSelectQuery>(*this); }

    std::vector<String> getPossibleNames(const String & name) const
    {
        std::vector<String> res;
        res.push_back(name);

        for (auto it = hidings.find(name); it != hidings.end(); it = hidings.find(it->second))
            res.push_back(it->second);
        return res;
    }

    static void hideNames(ASTSelectQuery & select, const std::vector<String> & hidden, const String & new_name)
    {
        if (!select.semantic)
            select.semantic = std::make_shared<SemanticSelectQuery>();

        auto & sema = static_cast<SemanticSelectQuery &>(*select.semantic);
        sema.hideNames(hidden, new_name);
    }

private:
    std::unordered_map<String, String> hidings;

    void hideNames(const std::vector<String> & hidden, const String & new_name)
    {
        for (auto & name : hidden)
            hidings.emplace(name, new_name);
    }
};

}
