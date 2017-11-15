#pragma once
#include <iostream>


namespace DB { class IBlockInputStream; }
std::ostream & operator<<(std::ostream & stream, const DB::IBlockInputStream & what);

namespace DB { class Field; }
std::ostream & operator<<(std::ostream & stream, const DB::Field & what);

namespace DB { struct NameAndTypePair; }
std::ostream & operator<<(std::ostream & stream, const DB::NameAndTypePair & what);

namespace DB { class IDataType; }
std::ostream & operator<<(std::ostream & stream, const DB::IDataType & what);

namespace DB { class IStorage; }
std::ostream & operator<<(std::ostream & stream, const DB::IStorage & what);

namespace DB { class TableStructureReadLock; }
std::ostream & operator<<(std::ostream & stream, const DB::TableStructureReadLock & what);

namespace DB { class IFunction; }
std::ostream & operator<<(std::ostream & stream, const DB::IFunction & what);

namespace DB { class Block; }
std::ostream & operator<<(std::ostream & stream, const DB::Block & what);

namespace DB { struct ColumnWithTypeAndName; }
std::ostream & operator<<(std::ostream & stream, const DB::ColumnWithTypeAndName & what);

namespace DB { class IColumn; }
std::ostream & operator<<(std::ostream & stream, const DB::IColumn & what);


namespace DB { struct SubqueryForSet; }
std::ostream & operator<<(std::ostream & stream, const DB::SubqueryForSet & what);

namespace DB { class IAST; }
std::ostream & operator<<(std::ostream & stream, const DB::IAST & what);

namespace DB { class ExpressionAnalyzer; }
std::ostream & operator<<(std::ostream & stream, const DB::ExpressionAnalyzer & what);


#include <Client/Connection.h>
std::ostream & operator<<(std::ostream & stream, const DB::Connection::Packet & what);

#include <Common/PODArray.h>
template <typename T, size_t INITIAL_SIZE, typename TAllocator, size_t pad_right_>
std::ostream & operator<<(std::ostream & stream, const DB::PODArray<T, INITIAL_SIZE, TAllocator, pad_right_> & what)
{
    stream << "PODArray(size = " << what.size() << ", capacity = " << what.capacity() << ")";
    dumpContainer(stream, what);
    return stream;
};


/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <common/iostream_debug_helpers.h>
