#pragma once

#include <memory>


namespace DB
{

class Context;
class AsynchronousMetrics;
class IDatabase;

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database);

}
