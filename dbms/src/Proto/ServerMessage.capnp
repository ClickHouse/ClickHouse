@0xfdd1e2948338b156;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("DB::Proto");

struct ColumnDefault
{
    kind @0 :UInt16;
    expression @1 :Text;
}

struct Column
{
    name @0 :Text;
    type @1 :Text;
    default @2 :ColumnDefault;
}

struct Table
{
    name @0 :Text;
    columns @1 :List(Column);
}

struct Database
{
    name @0 :Text;
    tables @1 :List(Table);
}

struct Context
{
    databases @0 :List(Database);
}
