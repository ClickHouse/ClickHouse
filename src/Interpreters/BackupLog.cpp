#include <Interpreters/BackupLog.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

BackupLogElement::BackupLogElement(BackupOperationInfo info_)
    : Base(std::move(info_))
    , event_time(std::chrono::system_clock::now())
    , event_time_usec(timeInMicroseconds(event_time))
{
}

NamesAndTypesList BackupLogElement::getNamesAndTypes()
{
    NamesAndTypesList names_and_types =
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
    };
    names_and_types.splice(end(names_and_types), Base::getNamesAndTypes());
    return names_and_types;
}

void BackupLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(DateLUT::instance().toDayNum(std::chrono::system_clock::to_time_t(event_time)).toUnderType());
    columns[i++]->insert(event_time_usec);
    Base::appendToBlock(columns, i);
}

}
