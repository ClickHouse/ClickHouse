#pragma once

#include <boost/noncopyable.hpp>

#include <mysqlxx/Connection.h>


namespace mysqlxx
{

/** Transaction RAII.
  * If commit() was not called before destroy, it will be rolled back.
  */
class Transaction : private boost::noncopyable
{
public:
    explicit Transaction(Connection & conn_)
        : conn(conn_), finished(false)
    {
        conn.query("START TRANSACTION").execute();
    }

    virtual ~Transaction()
    {
        try
        {
            if (!finished)
                rollback();
        }
        catch (...) /// NOLINT(bugprone-empty-catch)
        {
        }
    }

    void commit()
    {
        conn.query("COMMIT").execute();
        finished = true;
    }

    void rollback()
    {
        conn.query("ROLLBACK").execute();
        finished = true;
    }

private:
    Connection & conn;
    bool finished;
};


}
