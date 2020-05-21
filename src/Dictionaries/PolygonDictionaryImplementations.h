#pragma once

#include "PolygonDictionary.h"
#include "PolygonDictionaryUtils.h"

#include <vector>

namespace DB
{

/** Simple implementation of the polygon dictionary. Doesn't generate anything during its construction.
*   Iterates over all stored polygons for each query, checking each of them in linear time.
*   Retrieves the polygon with the smallest area containing the given point. If there is more than one any such polygon
*   may be returned.
*/
class SimplePolygonDictionary : public IPolygonDictionary
{
public:
    SimplePolygonDictionary(
            const std::string & database_,
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            InputType input_type_,
            PointType point_type_);

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    bool find(const Point & point, size_t & id) const override;
};

/** A polygon dictionary which generates a recursive grid in order to efficiently cut the number of polygons to be
 *  checked slowly for a given point. For more detail see the GridRoot class.
 *  Retrieves the polygon with the smallest area containing the given point. If there is more than one any such polygon
*   may be returned.
 */
class GridPolygonDictionary : public IPolygonDictionary
{
public:
    GridPolygonDictionary(
            const std::string & database_,
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            InputType input_type_,
            PointType point_type_);

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    bool find(const Point & point, size_t & id) const override;

    GridRoot<FinalCell> grid;
    static constexpr size_t kMinIntersections = 1;
    static constexpr size_t kMaxDepth = 5;
};

/** Smart implementation of the polygon dictionary. Uses BucketsPolygonIndex. */
class SmartPolygonDictionary : public IPolygonDictionary
{
public:
    SmartPolygonDictionary(
            const std::string & database_,
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            InputType input_type_,
            PointType point_type_);

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    bool find(const Point & point, size_t & id) const override;

    std::vector<BucketsPolygonIndex> buckets;
    GridRoot<FinalCell> grid;
    static constexpr size_t kMinIntersections = 1;
    static constexpr size_t kMaxDepth = 5;
};

/** Uses single BucketsPolygonIndex for all queries. */
class OneBucketPolygonDictionary : public IPolygonDictionary
{
public:
    OneBucketPolygonDictionary(
            const std::string & database_,
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            InputType input_type_,
            PointType point_type_);

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    bool find(const Point & point, size_t & id) const override;

    BucketsPolygonIndex buckets_idx;
    GridRoot<FinalCellWithSlabs> index;

    static constexpr size_t kMinIntersections = 1;
    static constexpr size_t kMaxDepth = 5;
};

}

