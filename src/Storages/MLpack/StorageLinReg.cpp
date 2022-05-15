#include <Storages/MLpack/StorageLinReg.h>

// #include <Poco/Version.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
// #include <Common/parseAddress.h>
// #include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>
// #include <QueryPipeline/Pipe.h>
#include <Processors/Sinks/SinkToStorage.h>
// #include <IO/BufferWithOwnMemory.h>
#include <Common/PODArray.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_SAVE_MLPACK_MODEL;
    extern const int INSUFFICIENT_NUMBER_OF_COLUMNS;
    extern const int CANNOT_CONVERT_TO_FLOAT64;
    extern const int FEATURE_BUFFERS_EMPTY;
}


class StorageLinRegSink : public SinkToStorage
{
public:
    StorageLinRegSink(
        const Block & sample_block_,
        String filepath_,
        std::shared_ptr<mlpack::regression::LinearRegression> model_)   
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , filepath(filepath_)
        , model(model_)
    {
        feature_buffers.resize(sample_block.columns());
    }

    String getName() const override { return "StorageLinRegSink"; }

    void consume(Chunk chunk) override
    {
        if (!chunk.hasRows())
            return;

        if (chunk.getNumColumns() < 2)
            throw Exception(
                "there must be at least 2 columns.",
                ErrorCodes::INSUFFICIENT_NUMBER_OF_COLUMNS); 

        auto& columns = chunk.getColumns();

        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
        {
            auto ptr = typeid_cast<const ColumnFloat64 *>(columns[i].get());
            if (ptr == nullptr)
                throw Exception(
                    "columns must be float64.",
                    ErrorCodes::CANNOT_CONVERT_TO_FLOAT64);

            for (size_t j = 0; j < chunk.getNumRows(); ++j)
            {
                feature_buffers[i].push_back(ptr->getFloat64(j));
            }
        }
        
    }

    void fillFeatureBuffer(const std::vector<PODArray<Float64>> & columns, size_t n_rows, size_t n_features, Float64* buffer) const
    {
        size_t offset = 0;
        for (size_t i = 0; i < n_features; ++i)
        {
            for (size_t j = 0; j < n_rows; ++j)
            {
                buffer[offset + j] = columns[i][j];
            }
            offset += n_rows;
        }
    }

    void onFinish() override
    {
        if (feature_buffers.empty())
            throw Exception(
                "your podarray vector is empty.",
                ErrorCodes::FEATURE_BUFFERS_EMPTY);

        if (feature_buffers.front().empty())
            return; // no data to train on
        size_t n_features = feature_buffers.size() - 1;
        size_t n_rows = feature_buffers.front().size();
        PODArray<Float64> feature_vector(n_features * n_rows);
        auto * buffer = feature_vector.data();

        fillFeatureBuffer(feature_buffers, n_rows, n_features, buffer);

        arma::mat regressors(buffer, n_rows, n_features);
        regressors = regressors.t();

        arma::rowvec target;
        target = arma::rowvec(feature_buffers.back().data(), n_rows);

        this->model->Train(regressors, target);

        bool save_status = mlpack::data::Save(this->filepath, "sraka", *this->model);
        if (!save_status) 
            throw Exception(
                "cannot save model.",
                ErrorCodes::CANNOT_SAVE_MLPACK_MODEL);
        
    }

private:
    Block sample_block;
    std::vector<PODArray<Float64>> feature_buffers;
    // std::vector<Memory<>> memory_buffers;
    String filepath;
    std::shared_ptr<mlpack::regression::LinearRegression> model;
};

StorageLinReg::StorageLinReg(
    const StorageID & table_id_,
    std::unique_ptr<LinRegSettings> linreg_settings_,
    const String filepath_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,    
    const String & comment)
    : IStorage(table_id_)
    , filepath(filepath_)
    , linreg_settings(std::move(linreg_settings_))
{
    StorageInMemoryMetadata storage_metadata; // отсавляем как есть
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    // if (linreg_settings== nullptr) 
    //     LOG_FATAL(&Poco::Logger::root(), "\n\n\n\n\n ZHOPA SETTINGS  {}\n\n\n\n\n", "status");

    
    model = std::make_shared<mlpack::regression::LinearRegression>();  
    // if (model== nullptr) 
    //     LOG_FATAL(&Poco::Logger::root(), "\n\n\n\n\n ZHOPA MODEL  {}\n\n\n\n\n", "status");
    
    model->Lambda() = linreg_settings->lambda.value;
}

SinkToStoragePtr StorageLinReg::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    auto sample_block = metadata_snapshot->getSampleBlock();

    return std::make_shared<StorageLinRegSink>(
        sample_block,
        this->filepath,
        this->model);
}


LinRegConfiguration StorageLinReg::getConfiguration(ASTs engine_args, ContextPtr context)
{
    LinRegConfiguration configuration;   
    
    if (engine_args.size() != 1)
        throw Exception(
            "wrong number of parameters.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

    configuration.filepath = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
    return configuration;
}


void registerStorageLinReg(StorageFactory & factory)
{
    factory.registerStorage("LinReg", [](const StorageFactory::Arguments & args)
    {
        auto linreg_settings = std::make_unique<LinRegSettings>();
        linreg_settings->loadFromQuery(*args.storage_def);
        auto configuration = StorageLinReg::getConfiguration(args.engine_args, args.getLocalContext());

        return std::make_shared<StorageLinReg>(
            args.table_id,
            std::move(linreg_settings),
            configuration.filepath,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .supports_settings = true,
        .source_access_type = AccessType::MLPACK,
    });

}

}
