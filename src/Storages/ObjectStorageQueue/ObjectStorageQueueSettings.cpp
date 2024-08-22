#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Common/Exception.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)

void ObjectStorageQueueSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            /// We support settings starting with s3_ for compatibility.
            for (auto & change : storage_def.settings->changes)
            {
                if (change.name.starts_with("s3queue_"))
                    change.name = change.name.substr(std::strlen("s3queue_"));
                if (change.name == "enable_logging_to_s3queue_log")
                    change.name = "enable_logging_to_queue_log";
            }

            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

}
