#include "TensorFlowModel.h"

#include <common/logger_useful.h>
#include <Common/SharedLibrary.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

struct TfLiteModel;
struct TfLiteTensor;
struct TfLiteInterpreterOptions;
struct TfLiteInterpreter;

typedef enum TfLiteStatus {
  kTfLiteOk = 0,

  // Generally referring to an error in the runtime (i.e. interpreter)
  kTfLiteError = 1,

  // Generally referring to an error from a TfLiteDelegate itself.
  kTfLiteDelegateError = 2,

  // Generally referring to an error in applying a delegate due to
  // incompatibility between runtime and delegate, e.g., this error is returned
  // when trying to apply a TfLite delegate onto a model graph that's already
  // immutable.
  kTfLiteApplicationError = 3
} TfLiteStatus;

// Types supported by tensor
typedef enum TfLiteType {
  kTfLiteNoType = 0,
  kTfLiteFloat32 = 1,
  kTfLiteInt32 = 2,
  kTfLiteUInt8 = 3,
  kTfLiteInt64 = 4,
  kTfLiteString = 5,
  kTfLiteBool = 6,
  kTfLiteInt16 = 7,
  kTfLiteComplex64 = 8,
  kTfLiteInt8 = 9,
  kTfLiteFloat16 = 10,
  kTfLiteFloat64 = 11,
  kTfLiteComplex128 = 12,
  kTfLiteUInt64 = 13,
  kTfLiteResource = 14,
  kTfLiteVariant = 15,
  kTfLiteUInt32 = 16,
} TfLiteType;

/// TensorFlowWrapperAPI wrapper interface functions.
struct TensorFlowWrapperAPI
{
    const char * (* TfLiteVersion)(void); // NO LINT
    TfLiteModel * (* TfLiteModelCreateFromFile)(const char * model_path); // NO LINT
    TfLiteInterpreterOptions * (* TfLiteInterpreterOptionsCreate)(); // NO LINT
    void (* TfLiteInterpreterOptionsSetNumThreads)(TfLiteInterpreterOptions * options, int32_t num_threads); // NO LINT
    TfLiteInterpreter * (* TfLiteInterpreterCreate)(const TfLiteModel * model, const TfLiteInterpreterOptions * optional_options); // NO LINT
    TfLiteStatus (* TfLiteInterpreterAllocateTensors)(TfLiteInterpreter * interpreter); // NO LINT
    TfLiteStatus (* TfLiteInterpreterInvoke)(TfLiteInterpreter * interpreter); // NO LINT
    
    void (* TfLiteInterpreterDelete)(TfLiteInterpreter * interpreter); // NO LINT
    void (* TfLiteInterpreterOptionsDelete)(TfLiteInterpreterOptions * options); // NO LINT
    void (* TfLiteModelDelete)(TfLiteModel * model); // NO LINT
    
    int32_t (* TfLiteInterpreterGetInputTensorCount)(const TfLiteInterpreter * interpreter); // NO LINT
    TfLiteTensor * (* TfLiteInterpreterGetInputTensor)(const TfLiteInterpreter * interpreter, int32_t input_index); // NO LINT
    TfLiteStatus (* TfLiteInterpreterResizeInputTensor)(TfLiteInterpreter * interpreter, int32_t input_index, const int * input_dims, int32_t input_dims_size); // NO LINT

    int32_t (* TfLiteInterpreterGetOutputTensorCount)(const TfLiteInterpreter * interpreter); // NO LINT
    const TfLiteTensor * (* TfLiteInterpreterGetOutputTensor)(const TfLiteInterpreter * interpreter, int32_t output_index); // NO LINT
  
    TfLiteType (* TfLiteTensorType)(const TfLiteTensor * tensor); // NO LINT
    const char * (* TfLiteTensorName)(const TfLiteTensor * tensor); // NO LINT
    size_t (* TfLiteTensorByteSize)(const TfLiteTensor * tensor); // NO LINT
    int32_t (* TfLiteTensorNumDims)(const TfLiteTensor * tensor); // NO LINT
    int32_t (* TfLiteTensorDim)(const TfLiteTensor * tensor, int32_t dim_index); // NO LINT
    TfLiteStatus (* TfLiteTensorCopyToBuffer)(const TfLiteTensor * output_tensor, void * output_data, size_t output_data_size); // NO LINT
    TfLiteStatus (* TfLiteTensorCopyFromBuffer)(TfLiteTensor * tensor, const void * input_data, size_t input_data_size); // NO LINT
};

namespace
{
/// Holds TensorFlow wrapper library and provides wrapper interface.
class TensorFlowLibHolder: public TensorFlowWrapperAPIProvider
{
public:
    explicit TensorFlowLibHolder(std::string lib_path_) : lib_path(std::move(lib_path_)), lib(lib_path) { initAPI(); }

    const TensorFlowWrapperAPI & getAPI() const override { return api; }
    const std::string & getCurrentPath() const { return lib_path; }

private:
    TensorFlowWrapperAPI api;
    std::string lib_path;
    SharedLibrary lib;

    template <typename T>
    void load(T& func, const std::string & name) { func = lib.get<T>(name); }

    void initAPI()
    {
        load(api.TfLiteVersion, "TfLiteVersion");
        load(api.TfLiteModelCreateFromFile, "TfLiteModelCreateFromFile");
        load(api.TfLiteInterpreterOptionsCreate, "TfLiteInterpreterOptionsCreate");
        load(api.TfLiteInterpreterOptionsSetNumThreads, "TfLiteInterpreterOptionsSetNumThreads");
        load(api.TfLiteInterpreterCreate, "TfLiteInterpreterCreate");
        load(api.TfLiteInterpreterAllocateTensors, "TfLiteInterpreterAllocateTensors");
        load(api.TfLiteInterpreterInvoke, "TfLiteInterpreterInvoke");

        load(api.TfLiteInterpreterOptionsDelete, "TfLiteInterpreterOptionsDelete");
        load(api.TfLiteInterpreterDelete, "TfLiteInterpreterDelete");
        load(api.TfLiteModelDelete, "TfLiteModelDelete");
        
        load(api.TfLiteInterpreterGetInputTensorCount, "TfLiteInterpreterGetInputTensorCount");
        load(api.TfLiteInterpreterGetInputTensor, "TfLiteInterpreterGetInputTensor");
        load(api.TfLiteInterpreterResizeInputTensor, "TfLiteInterpreterResizeInputTensor");

        load(api.TfLiteInterpreterGetOutputTensorCount, "TfLiteInterpreterGetOutputTensorCount");
        load(api.TfLiteInterpreterGetOutputTensor, "TfLiteInterpreterGetOutputTensor");

        load(api.TfLiteTensorType, "TfLiteTensorType");
        load(api.TfLiteTensorName, "TfLiteTensorName");
        load(api.TfLiteTensorByteSize, "TfLiteTensorByteSize");
        load(api.TfLiteTensorNumDims, "TfLiteTensorNumDims");
        load(api.TfLiteTensorDim, "TfLiteTensorDim");
        load(api.TfLiteTensorCopyToBuffer, "TfLiteTensorCopyToBuffer");
        load(api.TfLiteTensorCopyFromBuffer, "TfLiteTensorCopyFromBuffer");
        
    }
};

std::shared_ptr<TensorFlowLibHolder> getTensorFlowWrapperHolder(const std::string & lib_path)
{
    static std::shared_ptr<TensorFlowLibHolder> ptr;
    static std::mutex mutex;

    std::lock_guard lock(mutex);

    if (!ptr || ptr->getCurrentPath() != lib_path)
        ptr = std::make_shared<TensorFlowLibHolder>(lib_path);

    return ptr;
}

}

class TensorFlowModelImpl : public ITensorFlowModelImpl
{
public:
    TensorFlowModelImpl(std::string lib_path, std::string model_path)
    {
        api_provider = getTensorFlowWrapperHolder(lib_path);
        api = &api_provider->getAPI();

        model = api->TfLiteModelCreateFromFile(model_path.c_str());
        if (model == nullptr)
            throw Exception("Model loading was failed", ErrorCodes::BAD_ARGUMENTS);

        interpreter_options = api->TfLiteInterpreterOptionsCreate();

        interpreter = api->TfLiteInterpreterCreate(model, interpreter_options);
        if (interpreter == nullptr)
            throw Exception("Interpreter creating was failed", ErrorCodes::BAD_ARGUMENTS);
        
        tensors_count = api->TfLiteInterpreterGetInputTensorCount(interpreter);

        if (api->TfLiteInterpreterGetOutputTensorCount(interpreter) != 1)
            throw Exception("TensorFlow model must have one output tensor", ErrorCodes::BAD_ARGUMENTS);

        output_tensor = api->TfLiteInterpreterGetOutputTensor(interpreter, 0);
        output_tensor_type = api->TfLiteTensorType(output_tensor);

        switch(output_tensor_type)
        {
            case TfLiteType::kTfLiteFloat32: returnType = std::make_shared<DataTypeFloat32>(); break;
            case TfLiteType::kTfLiteFloat64: returnType = std::make_shared<DataTypeFloat64>(); break;
            case TfLiteType::kTfLiteInt8:    returnType = std::make_shared<DataTypeInt8>(); break;
            case TfLiteType::kTfLiteInt32:   returnType = std::make_shared<DataTypeInt32>(); break;
            case TfLiteType::kTfLiteInt64:   returnType = std::make_shared<DataTypeInt64>(); break;
            case TfLiteType::kTfLiteUInt8:   returnType = std::make_shared<DataTypeUInt8>(); break;
            case TfLiteType::kTfLiteUInt32:  returnType = std::make_shared<DataTypeUInt32>(); break;
            case TfLiteType::kTfLiteUInt64:  returnType = std::make_shared<DataTypeUInt64>(); break;
            default:
                throw Exception("Unsupported output tensor's data type", ErrorCodes::BAD_ARGUMENTS);
        }

        if (api->TfLiteInterpreterAllocateTensors(interpreter) != TfLiteStatus::kTfLiteOk)
            throw Exception("Allocating tensors was failed", ErrorCodes::BAD_ARGUMENTS);
    }

    ~TensorFlowModelImpl() override
    {
        if (interpreter != nullptr)
            api->TfLiteInterpreterDelete(interpreter);
        if (interpreter_options != nullptr)
            api->TfLiteInterpreterOptionsDelete(interpreter_options);
        if (model != nullptr)
            api->TfLiteModelDelete(model);
    }

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override
    {
        if (columns.empty())
            throw Exception("Got empty columns list for TensorFlow model", ErrorCodes::BAD_ARGUMENTS);
        
        if (columns.size() != static_cast<size_t>(tensors_count))
        {
            std::string msg;
            {
                WriteBufferFromString buffer(msg);
                buffer << "Number of columns is different with number of tensors: ";
                buffer << columns.size() << " vs " << tensors_count;
            }
            throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
        }

        switch(output_tensor_type)
        {
            case TfLiteType::kTfLiteFloat32: return evalImpl<ColumnFloat32, float>(columns);
            case TfLiteType::kTfLiteFloat64: return evalImpl<ColumnFloat64, double>(columns);
            case TfLiteType::kTfLiteInt8:    return evalImpl<ColumnInt8, int8_t>(columns);
            case TfLiteType::kTfLiteInt32:   return evalImpl<ColumnInt32, int32_t>(columns);
            case TfLiteType::kTfLiteInt64:   return evalImpl<ColumnInt64, int64_t>(columns);
            case TfLiteType::kTfLiteUInt8:   return evalImpl<ColumnUInt8, uint8_t>(columns);
            case TfLiteType::kTfLiteUInt32:  return evalImpl<ColumnUInt32, uint32_t>(columns);
            case TfLiteType::kTfLiteUInt64:  return evalImpl<ColumnUInt64, uint64_t>(columns);
            default:
                throw Exception("Unsupported output tensor's data type", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    DataTypePtr getReturnType() const override
    {
        return returnType;
    }

private:
    template<typename ColumnType, typename FieldType>
    ColumnPtr evalImpl(const ColumnRawPtrs & columns) const
    {
        size_t rows_count = columns.front()->size();
        auto res = ColumnType::create(rows_count);

        for (size_t i = 0; i < rows_count; ++i)
        {
            for (int32_t j = 0; j < tensors_count; ++j)
            {
                auto tensor = api->TfLiteInterpreterGetInputTensor(interpreter, j);
                if (tensor == nullptr)
                    throw Exception("Tensor getting was failed", ErrorCodes::BAD_ARGUMENTS);

                auto column = columns[j];
                if (!column->isNumeric())
                    throw Exception("Unsupported non-numeric data types", ErrorCodes::BAD_ARGUMENTS);

                auto copyToTensor = [this, &tensor](const auto& value) {
                    if (api->TfLiteTensorCopyFromBuffer(tensor, &value, sizeof(value)) != TfLiteStatus::kTfLiteOk) 
                        throw Exception("Copy to tensor was failed", ErrorCodes::BAD_ARGUMENTS);
                };

                auto type = api->TfLiteTensorType(tensor);
                switch(type)
                {
                    case TfLiteType::kTfLiteBool:    copyToTensor(column->getBool(i)); break;
                    case TfLiteType::kTfLiteFloat32: copyToTensor(column->getFloat32(i)); break;
                    case TfLiteType::kTfLiteFloat64: copyToTensor(column->getFloat64(i)); break;
                    case TfLiteType::kTfLiteInt8:    copyToTensor(static_cast<int8_t>(column->getInt(i))); break;
                    case TfLiteType::kTfLiteInt32:   copyToTensor(static_cast<int32_t>(column->getInt(i))); break;
                    case TfLiteType::kTfLiteInt64:   copyToTensor(column->getInt(i)); break;
                    case TfLiteType::kTfLiteUInt8:   copyToTensor(static_cast<uint8_t>(column->getUInt(i))); break;
                    case TfLiteType::kTfLiteUInt32:  copyToTensor(static_cast<uint32_t>(column->getUInt(i))); break;
                    case TfLiteType::kTfLiteUInt64:  copyToTensor(column->getUInt(i)); break;
                    default:
                        throw Exception("Unsupported input tensor's data type", ErrorCodes::BAD_ARGUMENTS);
                }
            }

            if (api->TfLiteInterpreterInvoke(interpreter) != TfLiteStatus::kTfLiteOk)
                throw Exception("Invoked was failed", ErrorCodes::BAD_ARGUMENTS);

            FieldType out_data;
            api->TfLiteTensorCopyToBuffer(output_tensor, &out_data, sizeof(FieldType));
            res->getElement(i) = out_data;
        }

        return res;
    } 

private:
    std::shared_ptr<TensorFlowWrapperAPIProvider> api_provider;
    const TensorFlowWrapperAPI * api;
    TfLiteModel * model;
    TfLiteInterpreterOptions * interpreter_options;
    TfLiteInterpreter * interpreter;
    const TfLiteTensor * output_tensor;
    TfLiteType output_tensor_type;
    DataTypePtr returnType;
    int32_t tensors_count;
};

TensorFlowModel::TensorFlowModel(std::string name_, std::string model_path_, std::string lib_path_,
                                 const ExternalLoadableLifetime & lifetime_)
    : name(std::move(name_))
    , model_path(std::move(model_path_))
    , lib_path(std::move(lib_path_))
    , lifetime(lifetime_)
{
    model = std::make_unique<TensorFlowModelImpl>(lib_path, model_path);
}

ColumnPtr TensorFlowModel::evaluate(const ColumnRawPtrs & columns) const
{
    if (!model)
        throw Exception("TensorFlowModel model was not loaded.", ErrorCodes::LOGICAL_ERROR);
    return model->evaluate(columns);
}

DataTypePtr TensorFlowModel::getReturnType() const
{
    return model->getReturnType();
}

const ExternalLoadableLifetime & TensorFlowModel::getLifetime() const
{
    return lifetime;
}

bool TensorFlowModel::isModified() const
{
    return true;
}

std::shared_ptr<const IExternalLoadable> TensorFlowModel::clone() const
{
    return std::make_shared<TensorFlowModel>(name, model_path, lib_path, lifetime);
}

}
