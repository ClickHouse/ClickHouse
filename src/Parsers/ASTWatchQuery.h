#pragma once
/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

class ASTWatchQuery : public ASTQueryWithTableAndOutput
{

public:
    ASTPtr limit_length;
    bool is_watch_events;

    ASTWatchQuery() = default;
    String getID(char) const override { return "WatchQuery_" + getDatabase() + "_" + getTable(); }

    ASTPtr clone() const override
    {
        std::shared_ptr<ASTWatchQuery> res = std::make_shared<ASTWatchQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneTableOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormattingBuffer & out) const override
    {
        out.writeKeyword("WATCH ");
        out.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

        if (is_watch_events)
            out.writeKeyword(" EVENTS");

        if (limit_length)
        {
            out.nlOrWs();
            out.writeIndent();
            out.writeKeyword("LIMIT ");
            limit_length->formatImpl(out);
        }
    }
};

}
