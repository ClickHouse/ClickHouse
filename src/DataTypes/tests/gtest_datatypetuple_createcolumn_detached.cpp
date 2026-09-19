#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationDetached.h>

#include <gtest/gtest.h>

using namespace DB;

/// Regression test: DataTypeTuple::createColumn must handle SerializationDetached
/// without throwing LOGICAL_ERROR.
///
/// SerializationDetached is an internal mechanism for parallel block marshalling.
/// It wraps the regular serialization but is not a SerializationWrapper subclass,
/// so the while-loop in DataTypeTuple::createColumn that peels SerializationWrapper
/// layers would skip it, causing typeid_cast<SerializationTuple*> to return null
/// and the old code to throw LOGICAL_ERROR (which aborts in debug builds).
///
/// The code path is reachable when a client connected via Native TCP protocol sends
/// a block where a Tuple column has has_custom=1 with DETACHED serialization kind (2).
/// The fuzzer found this via NativeReader with DBMS_TCP_PROTOCOL_VERSION.
TEST(DataTypeTupleCreateColumn, HandlesSerializationDetached)
{
    auto int32_type = std::make_shared<DataTypeInt32>();
    auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{int32_type, int32_type});

    auto default_serialization = tuple_type->getDefaultSerialization();
    auto detached_serialization = SerializationDetached::create(default_serialization);

    /// Before the fix this threw LOGICAL_ERROR (abort in debug/ASan builds).
    /// After the fix it calls createColumn(*nested) and returns a valid Tuple column.
    MutableColumnPtr column;
    EXPECT_NO_THROW(column = tuple_type->createColumn(*detached_serialization));
    EXPECT_NE(column, nullptr);
}
