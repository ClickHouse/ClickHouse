#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>

namespace DB
{

class ASTIndexAdvisorQuery : public IAST
{
public:
    ASTPtr queries;

    String getID(char) const override { return "IndexAdvisorQuery"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTIndexAdvisorQuery>(*this);
        res->children.clear();
        res->queries = queries->clone();
        res->children.push_back(res->queries);
        return res;
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        LOG_DEBUG(&Poco::Logger::get("ASTIndexAdvisorQuery"), "Formatting index advisor query");
        
        ostr << (settings.hilite ? hilite_keyword : "") << "ADVISE INDEX(" << (settings.hilite ? hilite_none : "");
        
        if (queries && !queries->children.empty())
        {
            for (size_t i = 0; i < queries->children.size(); ++i)
            {
                if (i > 0)
                    ostr << ", ";
                queries->children[i]->format(ostr, settings, state, frame);
            }
        }
        else
        {
            LOG_DEBUG(&Poco::Logger::get("ASTIndexAdvisorQuery"), "No queries found in index advisor query");
        }
        
        ostr << (settings.hilite ? hilite_keyword : "") << ")" << (settings.hilite ? hilite_none : "");
    }
};

} 
