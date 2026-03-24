#include "config.h"

#if USE_NLP

#include <Storages/System/StorageSystemStemmers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <libstemmer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_nlp_functions;
}

ColumnsDescription StorageSystemStemmers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the stemmer language"}
    };
}

StorageSystemStemmers::StorageSystemStemmers(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemStemmers::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    if (!context->getSettingsRef()[Setting::allow_experimental_nlp_functions])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "system.stemmers is an experimental table. "
            "Set `allow_experimental_nlp_functions` setting to enable it");

    for (const char ** language = sb_stemmer_list(); *language != nullptr; ++language)
        res_columns[0]->insert(String(*language));
}

}

#endif
