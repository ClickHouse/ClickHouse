#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/**
 * Parses queries like
 * SNAPSHOT {
 *     TABLE [db.]table_name
 *   | ALL
 *         [EXCEPT
 *             { TABLE table_name
 *             | TABLES table_name [, ...]
 *             | DATABASE database_name
 *             | DATABASES database_name [, ...]
 *             }
 *             [, ...]
 *         ]
 * } [, ...]
 * TO {
 *     S3('path/', ['aws_access_key_id', 'aws_secret_access_key'])
 *   | AzureBlobStorage('connection_string'|'storage_account_url', 'container_name', 'blobpath')
 * }
 */
class ParserSnapshotQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SNAPSHOT"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
