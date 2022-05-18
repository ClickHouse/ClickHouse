#include "MlpackModel.h"
#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <mlpack/methods/logistic_regression/logistic_regression.hpp>
#include <mlpack/methods/linear_svm/linear_svm.hpp>
#include <mlpack/core.hpp>

#include <Common/FieldVisitorConvertToNumber.h>
#include <mutex>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/PODArray.h>
#include <Common/SharedLibrary.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnsNumber.h>

//#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_LOAD_CATBOOST_MODEL;
extern const int CANNOT_APPLY_CATBOOST_MODEL;
}

class MlpackModelImpl
{
public:
    MlpackModelImpl(const std::string & model_path_, const std::string & method_) : model_path(model_path_), method(method_)
    {}

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const
    {
        if (columns.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got empty columns list for Mlpack model.");

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (!columns[i]->isNumeric())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} should be numeric to make float feature.", i);
            }
        }

        auto result = evalImpl(columns);
        return result;
    }

private:
    std::string model_path;
    std::string method;

    /// Buffer should be allocated with features_count * column->size() elements.
    template <typename T>
    void placeColumnAsNumber(const IColumn * column, T * buffer, size_t offset) const
    {
        size_t size = column->size();
        FieldVisitorConvertToNumber<T> visitor;
        for (size_t i = 0; i < size; ++i)
        {
            /// TODO: Replace with column visitor.
            Field field;
            column->get(i, field);
            buffer[offset + i] = applyVisitor(visitor, field);
        }
    }

    /// Place columns into buffer, returns column which holds placed data. Buffer should contains column->size() values.
    template <typename T>
    void placeNumericColumns(const ColumnRawPtrs & columns,
                                  size_t column_size, size_t columns_amount, double* buffer) const
    {
        if (column_size == 0)
            return;

        size_t result_offset = 0;
        
        for (size_t i = 0; i < columns_amount; ++i)
        {
            const auto * column = columns[i];
            if (column->isNumeric())
            {
                placeColumnAsNumber(column, buffer, result_offset);
                result_offset += column_size;
            }
        }
    }

    ColumnFloat64::MutablePtr evalImpl(const ColumnRawPtrs & columns) const
    {
        size_t columns_amount = columns.size();
        size_t column_size = columns.front()->size();

        if (column_size == 0 || columns_amount == 0)
        {
            return ColumnFloat64::create(column_size);
        }

        // double *features_buffer = new double[columns_amount * column_size];
        PODArray<double> float_features(columns_amount * column_size);
        auto * features_buffer = float_features.data();

        placeNumericColumns<float>(columns, column_size, columns_amount, features_buffer);

        arma::mat regressors(features_buffer, column_size, columns_amount);
        regressors = regressors.t();

        auto result = ColumnFloat64::create(column_size);
        auto * result_buf = result->getData().data();

        if (method == "linear")
        {
            mlpack::regression::LinearRegression lr;
            bool load_St = mlpack::data::Load(model_path, "model", lr);
            if (!load_St)
                throw Exception("Unable to parse Mlpack model from file.", ErrorCodes::CANNOT_LOAD_CATBOOST_MODEL);


            arma::rowvec answers(column_size);
            lr.Predict(regressors, answers);

            for (size_t i = 0; i < column_size; i++)
            {
                result_buf[i] = answers(i);
            }
        }
        if (method == "logistic")
        {
            mlpack::regression::LogisticRegression<arma::mat> lr;
            bool load_st = mlpack::data::Load(model_path, "model", lr);
            if (!load_st)
                throw Exception("Unable to parse Mlpack model from file.", ErrorCodes::CANNOT_LOAD_CATBOOST_MODEL);
            
            arma::Row<size_t> answers(column_size);
            lr.Classify(regressors, answers);

            for (size_t i = 0; i < column_size; i ++)
            {
                result_buf[i] = answers(i);
            }
        }

        if (method == "svm")
        {
            mlpack::svm::LinearSVM<arma::mat> lr;
            bool load_st = mlpack::data::Load(model_path, "model", lr);
            if (!load_st)
                throw Exception("Unable to parse Mlpack model from file.", ErrorCodes::CANNOT_LOAD_CATBOOST_MODEL);
            
            arma::Row<size_t> answers(column_size);
            lr.Classify(regressors, answers);

            for (size_t i = 0; i < column_size; i ++)
            {
                result_buf[i] = answers(i);
            }
        }

        return result;
    }
};


MlpackModel::MlpackModel(std::string name_, std::string model_path_,
                        std::string method_,
                        const ExternalLoadableLifetime & lifetime_)
    : name(std::move(name_)), model_path(std::move(model_path_)), method(std::move(method_)), lifetime(lifetime_)
{
    model = std::make_unique<MlpackModelImpl>(model_path, method);
}

MlpackModel::~MlpackModel() = default;

DataTypePtr MlpackModel::getReturnType() const
{
    auto type = std::make_shared<DataTypeFloat64>();
    return type;
}

ColumnPtr MlpackModel::evaluate(const ColumnRawPtrs & columns) const
{
    if (!model)
        throw Exception("Mlpack model was not loaded.", ErrorCodes::LOGICAL_ERROR);

    return model->evaluate(columns);
}

}
