#pragma once

#include <Core/Block.h>
#include <QueryPipeline/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <Interpreters/ISet.h>

#include <Common/SharedMutex.h>


namespace DB
{


/** Data structure for implementation of IN expression.
  */
class ProbSet : public ISet
{
public:
    /// 'fill_set_elements': in addition to hash table
    /// (that is useful only for checking that some value is in the set and may not store the original values),
    /// store all set elements in explicit form.
    /// This is needed for subsequent use for index.
    ProbSet(const SizeLimits & limits_, bool fill_set_elements_, bool transform_null_in_, 
            size_t size_of_filter_, String name_of_filter_, float precision_)
        : ISet(limits_, fill_set_elements_, transform_null_in_), 
        size_of_filter(size_of_filter_), name_of_filter(name_of_filter_), precision(precision_)
    {
    }

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    //void setHeader(const ColumnsWithTypeAndName & header) override;
   
protected:
    void initialize_data(ColumnRawPtrs key_columns) override;

private:

    //SetVariants data;

    size_t size_of_filter;
    String name_of_filter;
    float precision;

    template <typename Method>
      void insertFromBlockImpl(
          Method & method,
          const ColumnRawPtrs & key_columns,
          size_t rows,
          SetVariants & variants,
          ConstNullMapPtr null_map,
          ColumnUInt8::Container * out_filter);

      template <typename Method, bool has_null_map, bool build_filter>
      void insertFromBlockImplCase(
          Method & method,
          const ColumnRawPtrs & key_columns,
          size_t rows,
          SetVariants & variants,
          ConstNullMapPtr null_map,
          ColumnUInt8::Container * out_filter);

      template <typename Method>
      void executeImpl(
          Method & method,
          const ColumnRawPtrs & key_columns,
          ColumnUInt8::Container & vec_res,
          bool negative,
          size_t rows,
          ConstNullMapPtr null_map) const;

      template <typename Method, bool has_null_map>
      void executeImplCase(
          Method & method,
          const ColumnRawPtrs & key_columns,
          ColumnUInt8::Container & vec_res,
          bool negative,
          size_t rows,
          ConstNullMapPtr null_map) const;

// using SetPtr = std::shared_ptr<Set>;
// using ConstSetPtr = std::shared_ptr<const Set>;
// using Sets = std::vector<SetPtr>;

};

}
