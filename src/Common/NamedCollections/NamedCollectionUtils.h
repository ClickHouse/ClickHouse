#pragma once
#include <Interpreters/Context_fwd.h>

namespace Poco { namespace Util { class AbstractConfiguration; } }

namespace DB
{

class ASTCreateNamedCollectionQuery;
class ASTAlterNamedCollectionQuery;

namespace NamedCollectionUtils
{

enum class SourceId
{
    NONE = 0,
    CONFIG = 1,
    SQL = 2,
};

void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
void reloadFromConfig(const Poco::Util::AbstractConfiguration & config);

/// Load named collections from `context->getPath() / named_collections /`.
void loadFromSQL(ContextPtr context);

/// Remove collection as well as its metadata from `context->getPath() / named_collections /`.
void removeFromSQL(const std::string & collection_name, ContextPtr context);
void removeIfExistsFromSQL(const std::string & collection_name, ContextPtr context);

/// Create a new collection from AST and put it to `context->getPath() / named_collections /`.
void createFromSQL(const ASTCreateNamedCollectionQuery & query, ContextPtr context);

/// Update definition of already existing collection from AST and update result in `context->getPath() / named_collections /`.
void updateFromSQL(const ASTAlterNamedCollectionQuery & query, ContextPtr context);

void loadIfNot();

}

}
