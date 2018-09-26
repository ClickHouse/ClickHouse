/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN col_name type [AFTER col_after],]
  *     [DROP COLUMN col_to_drop, ...]
  *     [CLEAR COLUMN col_to_clear [IN PARTITION partition],]
  *     [MODIFY COLUMN col_to_modify type, ...]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [ADD TO PARAMETER param_name value, ...]
  *     [DROP FROM PARAMETER param_name value, ...]
  *     [MODIFY PARAMETER param_name value, ...]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE PARTITION]
  *     [DELETE WHERE ...]
  *     [UPDATE col_name = expr, ... WHERE ...]
  * ALTER CHANNEL [db.name] [ON CLUSTER cluster]
  *     [ADD live_view, ...]
  *     [DROP live_view, ...]
  *     [SUSPEND live_view, ...]
  *     [RESUME live_view, ...]
  *     [REFRESH live_view, ...]
  *     [MODIFY live_view, ...]
  */

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserAlterCommandList : public IParserBase
{
protected:
    const char * getName() const { return "a list of ALTER commands"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const { return "ALTER command"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/// Part of the UPDATE command of the form: col_name = expr
class ParserAssignment : public IParserBase
{
protected:
    const char * getName() const { return "column assignment"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
