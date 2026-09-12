#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemTableEngines.h>

namespace DB
{

ColumnsDescription StorageSystemTableEngines::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of table engine."},
        {"supports_settings", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports SETTINGS clause."},
        {"supports_skipping_indices", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports skipping indices."},
        {"supports_projections", std::make_shared<DataTypeUInt8>(), "Flag that indicated if table engine supports projections."},
        {"supports_sort_order", std::make_shared<DataTypeUInt8>(),
            "Flag that indicates if table engine supports clauses PARTITION_BY, PRIMARY_KEY, ORDER_BY and SAMPLE_BY."
        },
        {"supports_ttl", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports TTL."},
        {"supports_replication", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports data replication."},
        {"supports_deduplication", std::make_shared<DataTypeUInt8>(), "Flag that indicates if table engine supports data deduplication."},
        {"supports_parallel_insert", std::make_shared<DataTypeUInt8>(),
            "Flag that indicates if table engine supports parallel insert (see max_insert_threads setting)."
        },
        {"description", std::make_shared<DataTypeString>(),
            "A description of what the table engine does. "
            "For engines that have a dedicated documentation page, this contains the full Markdown body of that page; "
            "for the remaining engines it is a concise summary."
        },
        {"syntax", std::make_shared<DataTypeString>(),
            "How the table engine is used when creating a table. "
            "For most engines this is the ENGINE clause of a CREATE TABLE query, "
            "but some engines (such as the various kinds of views or the Loop engine) are used through other forms, "
            "such as CREATE VIEW or a SELECT from a table function."
        },
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the table engine was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related table engines."},
    };
}

void StorageSystemTableEngines::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & pair : StorageFactory::instance().getAllStorages())
    {
        const auto & documentation = pair.second.documentation;

        int i = 0;
        res_columns[i++]->insert(pair.first);
        res_columns[i++]->insert(pair.second.features.supports_settings);
        res_columns[i++]->insert(pair.second.features.supports_skipping_indices);
        res_columns[i++]->insert(pair.second.features.supports_projections);
        res_columns[i++]->insert(pair.second.features.supports_sort_order);
        res_columns[i++]->insert(pair.second.features.supports_ttl);
        res_columns[i++]->insert(pair.second.features.supports_replication);
        res_columns[i++]->insert(pair.second.features.supports_deduplication);
        res_columns[i++]->insert(pair.second.features.supports_parallel_insert);
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & name : documentation.related)
            related.push_back(name);
        res_columns[i++]->insert(related);
    }
}

}
