#pragma once

// #include <Disks/IDisk.h>
// #include <Poco/Util/Application.h>
// #include <IO/WriteBufferFromFileDescriptor.h>
// #include <IO/ReadBufferFromFileDescriptor.h>
// #include <IO/copyData.h>
// #include <boost/program_options.hpp>
// #include <Common/TerminalSize.h>
#include <Storages/MLpack/ModelSettings.h>
#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <mlpack/methods/logistic_regression/logistic_regression.hpp>
#include <mlpack/methods/linear_svm/linear_svm.hpp>
#include <mlpack/core.hpp>
#include <ensmallen.hpp>
#include <Common/PODArray.h>
#include <Common/logger_useful.h>

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

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_LOAD_CATBOOST_MODEL;
extern const int CANNOT_APPLY_CATBOOST_MODEL;
}

namespace DB
{

struct TrainResult {};

struct LinearRegressionResult : public TrainResult
{
   const mlpack::regression::LinearRegression& model;
};

struct LogisticRegressionResult : public TrainResult
{
   const mlpack::regression::LogisticRegression<>& model;
};

struct LinearSVMResult : public TrainResult
{
   const mlpack::svm::LinearSVM<>& model;
};


class IModel
{
public:
    IModel() = default;
    virtual ~IModel() = default;

    virtual String GetName() const = 0;

    virtual TrainResult GetModel() const = 0;

    virtual void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) = 0;
    
    virtual bool Save(String filepath) = 0;

    virtual ColumnPtr evaluateModel(const ColumnRawPtrs & columns) = 0;

protected:
    void checkColumns(const ColumnRawPtrs & columns)
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
    }

    arma::mat prepareTestDataset(const ColumnRawPtrs & columns)
    {
        size_t columns_amount = columns.size();
        size_t column_size = columns.front()->size();

        PODArray<double> float_features(columns_amount * column_size);
        auto * features_buffer = float_features.data();

        placeNumericColumns<float>(columns, column_size, columns_amount, features_buffer);

        arma::mat regressors(features_buffer, column_size, columns_amount);
        regressors = regressors.t();

        return regressors;
    }

private:
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

};

class LinReg: public IModel 
{
public:
    LinReg(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
    } 

    String GetName() const override { return "LinearRegression"; }

    TrainResult GetModel() const override { return LinearRegressionResult{{}, model}; }

    void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) override
    {
        arma::rowvec target = arma::rowvec(target_ptr, n_rows);
        model.Train(regressors, target);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "linreg", model);
    }

    ColumnPtr evaluateModel(const ColumnRawPtrs & columns) override
    {
        checkColumns(columns);
        size_t column_size = columns.front()->size();

        if (column_size == 0) {
            return ColumnFloat64::create(column_size);
        }

        auto result = ColumnFloat64::create(column_size);
        auto * result_buf = result->getData().data();

        arma::mat regressors = prepareTestDataset(columns);

        arma::rowvec answers(column_size);
        model.Predict(regressors, answers);

        for (size_t i = 0; i < column_size; i++)
        {
            result_buf[i] = answers(i);
        }

        return result;
    }

protected:
    mlpack::regression::LinearRegression model;    
};

class LogReg: public IModel 
{
public:
    LogReg(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
        // auto opt_type = model_settings->optimizer.value;
    } 

    String GetName() const override { return "LogisticRegression"; }

    TrainResult GetModel() const override { return LogisticRegressionResult{{}, model}; }

    // void Train(const arma::mat& regressors, const arma::Row<double>& target) override {
    //     ens::L_BFGS lbfgsOpt;

    //     model.Train(regressors, target, lbfgsOpt);
    // }

    void TrainArb(const arma::mat& regressors, Float64 * target_ptr, size_t n_rows) override {
        PODArray<size_t> modified_target(n_rows);
        for (size_t i = 0; i < n_rows; ++i)
        {
            modified_target[i] = static_cast<size_t>(target_ptr[i]);
        }
        // size_t * modified_ptr = reinterpret_cast<size_t *>(target_ptr); 
        arma::Row<size_t> target = arma::Row<size_t>(modified_target.data(), n_rows);
        model.Train(regressors, target);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "logreg", model);
    }

    ColumnPtr evaluateModel(const ColumnRawPtrs & columns) override
    {
        checkColumns(columns);
        size_t column_size = columns.front()->size();

        if (column_size == 0) {
            return ColumnFloat64::create(column_size);
        }

        auto result = ColumnFloat64::create(column_size);
        auto * result_buf = result->getData().data();

        arma::mat regressors = prepareTestDataset(columns);

        arma::rowvec answers(column_size);
        model.Classify(regressors, answers);

        for (size_t i = 0; i < column_size; i++)
        {
            result_buf[i] = answers(i);
        }

        return result;
    }

protected:
    mlpack::regression::LogisticRegression<> model;

};

class LinearSVM: public IModel 
{
public:
    LinearSVM(
        std::unique_ptr<ModelSettings> model_settings)
        : IModel()
    {
        model.Lambda() = model_settings->lambda.value;
        model.Delta() = model_settings->delta.value;
    } 

    String GetName() const override { return "LinearSVM"; }

    TrainResult GetModel() const override { return LinearSVMResult{{}, model}; }

    // void Train(const arma::mat& regressors, const arma::Row<double>& target) override {
    //     ens::L_BFGS lbfgsOpt;
    //     model.Train(regressors, target, 2, lbfgsOpt);
    // }

    void TrainArb(const arma::mat& regressors, Float64 * /*target_ptr*/, size_t n_rows) override 
    {
        // PODArray<size_t> modified_target;
        // modified_target.resize(n_rows);
        // if (target_ptr == nullptr) 
        //     LOG_FATAL(&Poco::Logger::root(), "\n\n\n\n\n ZHOPA target ptr  {}\n\n\n\n\n", "status");

        
        // for (size_t i = 0; i < n_rows; ++i)
        // {
        //     modified_target[i] = static_cast<size_t>(target_ptr[i]);
        // }
        // size_t * modified_ptr = reinterpret_cast<size_t *>(target_ptr); 
        PODArray<size_t> pipa(n_rows, 1);
        arma::Row<size_t> target = arma::Row<size_t>(pipa.data(), n_rows);
        // arma::Row<size_t> target = arma::Row<size_t>(modified_target.data(), n_rows);
        ens::L_BFGS lbfgsOpt;
        model.Train(regressors, target, 2, lbfgsOpt);
    }

    bool Save(String filepath) override
    {
        return mlpack::data::Save(filepath, "linearsvm", model);
    }

    ColumnPtr evaluateModel(const ColumnRawPtrs & columns) override
    {
        checkColumns(columns);
        size_t column_size = columns.front()->size();

        if (column_size == 0) {
            return ColumnFloat64::create(column_size);
        }

        auto result = ColumnFloat64::create(column_size);
        auto * result_buf = result->getData().data();

        arma::mat regressors = prepareTestDataset(columns);

        arma::rowvec answers(column_size);
        model.Classify(regressors, answers);

        for (size_t i = 0; i < column_size; i++)
        {
            result_buf[i] = answers(i);
        }

        return result;
    }

protected:
    mlpack::svm::LinearSVM<> model;    
};

using IModelPtr = std::shared_ptr<IModel>;

}
