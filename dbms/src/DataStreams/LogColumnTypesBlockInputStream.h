
#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>
#include <Columns/ColumnVisitor.h>
#include <Columns/ColumnString.h>


namespace DB
{

/** Removes the specified columns from the block.
    */
class LogColumnTypesBlockInputStream : public IProfilingBlockInputStream, public ColumnVisitorImpl<LogColumnTypesBlockInputStream>
{
public:
    LogColumnTypesBlockInputStream(
        BlockInputStreamPtr input_) : log(&Poco::Logger::get("LogColumnTypesBlockInputStream"))
    {
        children.push_back(input_);
    }

    String getName() const override { return "LogColumnTypes"; }

    String getID() const override
    {
        std::stringstream res;
        res << "LogColumnTypes";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        for (const auto & it : res.getColumns())
            it.column->accept(*this);
        return res;
    }

public:
    template <typename T>
    void visitImpl(T & column) { print_log(column); }
    template <typename T>
    void visitImpl(const T & column) { print_log(column); }

private:
    void print_log(const ColumnString & column)
    {
        LOG_TRACE(log, typeid(ColumnString).name());
    }

    void print_log(ColumnString & column)
    {
        LOG_TRACE(log, typeid(ColumnString).name());
    }

    template <typename T>
    void print_log(T & column)
    {
        LOG_TRACE(log, " other type");
    }

    template <typename T>
    void print_log(const T & column)
    {
        LOG_TRACE(log, " other type");
    }


    Names columns_to_remove;
    Poco::Logger * log;
};

}
