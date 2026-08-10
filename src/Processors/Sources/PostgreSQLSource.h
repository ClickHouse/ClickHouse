#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <Core/ExternalResultDescription.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/PostgreSQL/Utils.h>

#include <mutex>


namespace DB
{

template <typename T = pqxx::ReadTransaction>
class PostgreSQLSource : public ISource
{

public:
    /// structure_hint_, if set, is appended to the error of a failed value conversion. Callers that
    /// map the result positionally use it to state the column order the result must be in.
    PostgreSQLSource(
        postgres::ConnectionHolderPtr connection_holder_,
        const String & query_str_,
        SharedHeader sample_block,
        UInt64 max_block_size_,
        const String & structure_hint_ = {});

    String getName() const override { return "PostgreSQL"; }

    ~PostgreSQLSource() override;

protected:
    PostgreSQLSource(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        SharedHeader sample_block,
        UInt64 max_block_size_,
        bool auto_commit_);

    Status prepare() override;

    Chunk generate() override;

    void onCancel() noexcept override;

    void onStart();

private:
    void init(const Block & sample_block);

    void finalize(const std::shared_ptr<T> & tx_to_cancel, pqxx::stream_from * stream_to_close) noexcept;

    const UInt64 max_block_size;
    bool auto_commit = true;
    ExternalResultDescription description;
    const String structure_hint;

    std::atomic<bool> started{false};
    /// Asks the read to stop. A signal only: it never takes the teardown from whoever owes it.
    std::atomic<bool> stop_requested{false};
    /// Claimed once by whoever tears the connection down: prepare() on an uncancelled finish, else
    /// the destructor. onCancel()'s interrupt does not claim it - it cannot close the stream.
    std::atomic<bool> finalized{false};

    /// tx and stream are written only by the pipeline thread; this is for onCancel() to read tx.
    std::mutex tx_mutex;
    /// Serializes the cancel_query() in finalize() between onCancel() and the destructor.
    std::mutex cancel_mutex;

    postgres::ConnectionHolderPtr connection_holder;

    UnorderedMapWithMemoryTracking<size_t, PostgreSQLArrayInfo> array_info;

protected:
    String query_str;
    /// tx and stream must be destroyed before connection_holder.
    std::shared_ptr<T> tx;
    std::unique_ptr<pqxx::stream_from> stream;
};


/// Passes transaction object into PostgreSQLSource and does not close transaction after read is finished.
template <typename T>
class PostgreSQLTransactionSource final : public PostgreSQLSource<T>
{
public:
    using Base = PostgreSQLSource<T>;

    PostgreSQLTransactionSource(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        SharedHeader sample_block_,
        const UInt64 max_block_size_)
        : PostgreSQLSource<T>(tx_, query_str_, sample_block_, max_block_size_, false) {}
};

}

#endif
