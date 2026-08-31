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
#include <Common/SipHash.h>
#include <Common/quoteString.h>


namespace Poco::JSON { class Object; }

namespace DB
{

class ASTWatchQuery : public ASTQueryWithTableAndOutput
{

public:
    ASTPtr limit_length;
    bool is_watch_events = false;

    ASTWatchQuery() = default;
    String getID(char) const override { return "WatchQuery_" + getDatabase() + "_" + getTable(); }

    ASTPtr clone() const override
    {
        boost::intrusive_ptr<ASTWatchQuery> res = make_intrusive<ASTWatchQuery>(*this);
        res->children.clear();
        /// `limit_length` is not a child: the parser puts it into the member only. Do not leave it
        /// shared with the source.
        if (limit_length)
            res->limit_length = limit_length->clone();
        /// The parser adds the database/table children first and `ParserQueryWithOutput` appends
        /// the output options last; reproduce that order so the clone has the same tree hash.
        cloneTableOptions(*res);
        cloneOutputOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override
    {
        /// Neither `limit_length` nor `is_watch_events` is a child, so without hashing them
        /// `WATCH t`, `WATCH t EVENTS` and `WATCH t LIMIT 5` would all hash the same.
        hash_state.update(is_watch_events);
        hash_state.update(limit_length != nullptr);
        if (limit_length)
            limit_length->updateTreeHash(hash_state, ignore_aliases);
        ASTQueryWithTableAndOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

        ostr << "WATCH ";

        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);

        if (is_watch_events)
        {
            ostr << " " << "EVENTS";
        }

        if (limit_length)
        {
            ostr << settings.nl_or_ws << indent_str << "LIMIT ";
            limit_length->format(ostr, settings, state, frame);
        }
    }
};

}
