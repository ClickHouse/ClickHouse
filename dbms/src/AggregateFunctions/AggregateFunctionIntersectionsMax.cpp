#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionIntersectionsMax.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

template <typename T>
typename Intersections<T>::PointsMap::iterator
Intersections<T>::insert_point(const T &v)
{
    auto res = points.emplace(v,0);
    auto &i = res.first;
    if(!res.second) return i;
    if(i==points.begin()) return i;
    auto prev = i;
    prev--;
    i->second=prev->second;
    return i;
}

template <typename T>
void Intersections<T>::add(const T &start, const T &end, T weight)
{
    auto sp = insert_point(start);
    auto ep = end ? insert_point(end) : points.end();
    do {
        sp->second+=weight;
        if(sp->second > max_weight) {
            max_weight = sp->second;
            max_weight_pos = sp->first;
        }
    } while(++sp != ep);
}

template <typename T>
void Intersections<T>::merge(const Intersections &other)
{
    if(other.points.empty())
        return;

    typename PointsMap::const_iterator prev, i = other.points.begin();
    prev = i;
    i++;

    while(i != other.points.end()) {
        add(prev->first,i->first,prev->second);
        prev = i;
        i++;
    }

    if(prev != other.points.end())
        add(prev->first,0,prev->second);
}

template <typename T>
void Intersections<T>::serialize(WriteBuffer & buf) const
{
    writeBinary(points.size(),buf);
    for(const auto &p: points) {
        writeBinary(p.first,buf);
        writeBinary(p.second,buf);
    }
}

template <typename T>
void Intersections<T>::deserialize(ReadBuffer & buf)
{
    std::size_t size;
    T point;
    T weight;

    readBinary(size, buf);
    for (std::size_t i = 0; i < size; ++i) {
        readBinary(point, buf);
        readBinary(weight,buf);
        points.emplace(point,weight);
    }
}

void AggregateFunctionIntersectionsMax::_add(
        AggregateDataPtr place,
        const IColumn & column_start,
        const IColumn & column_end,
        size_t row_num) const
{
    PointType start_time, end_time;
    Field tmp_start_time_field, tmp_end_time_field;

    column_start.get(row_num,tmp_start_time_field);
    if(tmp_start_time_field.isNull())
        return;
    start_time = tmp_start_time_field.template get<PointType>();
    if(0==start_time)
        return;

    column_end.get(row_num,tmp_end_time_field);
    if(tmp_end_time_field.isNull()) {
        end_time = 0;
    } else {
        end_time = tmp_end_time_field.template get<PointType>();
        if(0!=end_time) {
            if(end_time==start_time) {
                end_time = 0;
            } else if(end_time < start_time) {
                return;
            }
        }
    }

    data(place).add(start_time,end_time);
}

namespace
{

AggregateFunctionPtr createAggregateFunctionIntersectionsMax(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    return std::make_shared<AggregateFunctionIntersectionsMax>(argument_types,parameters,false);
}

AggregateFunctionPtr createAggregateFunctionIntersectionsMaxPos(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    return std::make_shared<AggregateFunctionIntersectionsMax>(argument_types,parameters,true);
}

}

void registerAggregateFunctionIntersectionsMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("intersectionsMax",
        createAggregateFunctionIntersectionsMax,
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("intersectionsMaxPos",
        createAggregateFunctionIntersectionsMaxPos,
        AggregateFunctionFactory::CaseInsensitive);
}

}

