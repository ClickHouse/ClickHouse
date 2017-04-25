#include "iostream_debug_helpers.h"

#include <iostream>


#include <DataStreams/IBlockInputStream.h>
std::ostream & operator<<(std::ostream & stream, const DB::IBlockInputStream & what)
{
    stream << "IBlockInputStream(id = " << what.getID() << ", name = " << what.getName() << ")";
    //what.dumpTree(stream); // todo: set const
    return stream;
}


#include <Core/NamesAndTypes.h>
std::ostream & operator<<(std::ostream & stream, const DB::NameAndTypePair & what)
{
    stream << "NameAndTypePair(name = " << what.name << ", type = " << what.type << ")";
    return stream;
}


#include <Core/Field.h>
std::ostream & operator<<(std::ostream & stream, const DB::Field & what)
{
    stream << "Field(type = " << what.getTypeName() << ")";
    return stream;
}


#include <DataTypes/IDataType.h>
std::ostream & operator<<(std::ostream & stream, const DB::IDataType & what)
{
    stream << "IDataType(name = " << what.getName() << ", default = " << what.getDefault() << ", isNullable = " << what.isNullable()
           << ", isNumeric = " << what.isNumeric() << ", behavesAsNumber = " << what.behavesAsNumber() << ")";
    return stream;
}


#include <Storages/IStorage.h>
std::ostream & operator<<(std::ostream & stream, const DB::IStorage & what)
{
    stream << "IStorage(name = " << what.getName() << "tableName" << what.getTableName() << ")"
           << " {" << what.getColumnsList() << "}";
    // isRemote supportsSampling supportsFinal supportsPrewhere supportsParallelReplicas
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::TableStructureReadLock & what)
{
    stream << "TableStructureReadLock()";
    return stream;
}
