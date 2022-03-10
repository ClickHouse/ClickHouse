#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTAlterQuery.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN [IF NOT EXISTS] col_name type [AFTER col_after],]
  *     [DROP COLUMN [IF EXISTS] col_to_drop, ...]
  *     [CLEAR COLUMN [IF EXISTS] col_to_clear[ IN PARTITION partition],]
  *     [MODIFY COLUMN [IF EXISTS] col_to_modify type, ...]
  *     [RENAME COLUMN [IF EXISTS] col_name TO col_name]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [MODIFY SETTING setting_name=setting_value, ...]
  *     [RESET SETTING setting_name, ...]
  *     [COMMENT COLUMN [IF EXISTS] col_name string]
  *     [MODIFY COMMENT string]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE [PARTITION] [WITH NAME name]]
  *     [DELETE[ IN PARTITION partition] WHERE ...]
  *     [UPDATE col_name = expr, ...[ IN PARTITION partition] WHERE ...]
  *     [ADD INDEX [IF NOT EXISTS] index_name [AFTER index_name]]
  *     [DROP INDEX [IF EXISTS] index_name]
  *     [CLEAR INDEX [IF EXISTS] index_name IN PARTITION partition]
  *     [MATERIALIZE INDEX [IF EXISTS] index_name [IN PARTITION partition]]
  * ALTER LIVE VIEW [db.name]
  *     [REFRESH]
  *
  * CREATE INDEX [IF NOT EXISTS] name ON [db].name (expression) TYPE type GRANULARITY value
  * DROP INDEX [IF EXISTS] name on [db].name
  */

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserAlterCommandList : public IParserBase
{
protected:
    const char * getName() const  override{ return "a list of ALTER commands"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ASTAlterQuery::AlterObjectType alter_object;
    ASTAlterCommand::Type  command_type;

    ParserAlterCommandList(ASTAlterQuery::AlterObjectType alter_object_ = ASTAlterQuery::AlterObjectType::TABLE, ASTAlterCommand::Type command_type_= ASTAlterCommand::NO_TYPE)
            : alter_object(alter_object_), command_type(command_type_) {}
};


class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER command"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ASTAlterQuery::AlterObjectType alter_object;
    ASTAlterCommand::Type  command_type;

    ParserAlterCommand(ASTAlterQuery::AlterObjectType alter_object_ = ASTAlterQuery::AlterObjectType::TABLE, ASTAlterCommand::Type command_type_= ASTAlterCommand::NO_TYPE)
            : alter_object(alter_object_), command_type(command_type_) {}
};


/** Part of CREATE INDEX expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserCreateIndexDeclaration : public IParserBase
{
public:
    ParserCreateIndexDeclaration() {}

protected:
    const char * getName() const override { return "index declaration in create index"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
