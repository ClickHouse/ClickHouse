#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define S3QUEUE_RELATED_SETTINGS(M, ALIAS) \
    M(String, mode, "unordered", "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKepeer", 0) \
    M(String, keeper_path, "/", "Zookeeper node path", 0) \

#define LIST_OF_S3QUEUE_SETTINGS(M, ALIAS) \
    S3QUEUE_RELATED_SETTINGS(M, ALIAS) \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(S3QueueSettingsTraits, LIST_OF_S3QUEUE_SETTINGS)


struct S3QueueSettings : public BaseSettings<S3QueueSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
