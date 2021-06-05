#pragma once

#include <memory>


namespace DB
{

class Context;
class AsynchronousMetrics;
class IDatabase;

void attachInformationSchemaLocal(IDatabase & information_schema_database);

}
