#pragma once

#include <Common/config.h>

#if USE_CASSANDRA
#include <cassandra.h>
#include <utility>
#include <memory>

namespace DB
{

namespace Cassandra
{

template<typename CassT>
CassT * defaultCtor() { return nullptr; }

/// RAII wrapper for raw pointers to objects from cassandra driver library
template<typename CassT, auto Dtor, auto Ctor = defaultCtor<CassT>>
class ObjectHolder
{
    CassT * ptr = nullptr;
public:
    template<typename... Args>
    ObjectHolder(Args &&... args) : ptr(Ctor(std::forward<Args>(args)...)) {} /// NOLINT
    ObjectHolder(CassT * ptr_) : ptr(ptr_) {} /// NOLINT

    ObjectHolder(const ObjectHolder &) = delete;
    ObjectHolder & operator = (const ObjectHolder &) = delete;

    ObjectHolder(ObjectHolder && rhs) noexcept : ptr(rhs.ptr) { rhs.ptr = nullptr; }
    ObjectHolder & operator = (ObjectHolder && rhs) noexcept
    {
        if (ptr)
            Dtor(ptr);
        ptr = rhs.ptr;
        rhs.ptr = nullptr;
        return *this;
    }

    ~ObjectHolder()
    {
        if (ptr)
            Dtor(ptr);
    }

    /// For implicit conversion when passing object to driver library functions
    operator CassT * () { return ptr; } /// NOLINT
    operator const CassT * () const { return ptr; } /// NOLINT
};

}

/// These object are created on pointer construction
using CassClusterPtr = Cassandra::ObjectHolder<CassCluster, cass_cluster_free, cass_cluster_new>;
using CassStatementPtr = Cassandra::ObjectHolder<CassStatement, cass_statement_free, cass_statement_new>;
using CassSessionPtr = Cassandra::ObjectHolder<CassSession, cass_session_free, cass_session_new>;

/// Share connections between streams. Executing statements in one session object is thread-safe
using CassSessionShared = std::shared_ptr<CassSessionPtr>;
using CassSessionWeak = std::weak_ptr<CassSessionPtr>;

/// The following objects are created inside Cassandra driver library,
/// but must be freed by user code
using CassFuturePtr = Cassandra::ObjectHolder<CassFuture, cass_future_free>;
using CassResultPtr = Cassandra::ObjectHolder<const CassResult, cass_result_free>;
using CassIteratorPtr = Cassandra::ObjectHolder<CassIterator, cass_iterator_free>;

/// Checks return code, throws exception on error
void cassandraCheck(CassError code);
void cassandraWaitAndCheck(CassFuturePtr & future);

/// By default driver library prints logs to stderr.
/// It should be redirected (or, at least, disabled) before calling other functions from the library.
void setupCassandraDriverLibraryLogging(CassLogLevel level);

void cassandraLogCallback(const CassLogMessage * message, void * data);

}

#endif
