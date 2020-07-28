#pragma once

#include <boost/noncopyable.hpp>

#include <mysqlxx/Connection.h>


namespace mysqlxx
{

/** RAII для транзакции. При инициализации, транзакция стартует.
  * При уничтожении, если не был вызван метод commit(), будет произведёт rollback.
  */
class Transaction : private boost::noncopyable
{
public:
    Transaction(Connection & conn_)
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
        catch (...)
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
