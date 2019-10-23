#include <Databases/DatabaseAtomic.h>


namespace DB
{

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, const Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
}



}

