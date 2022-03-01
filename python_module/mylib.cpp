#include <cstddef>
#include <string>
#include <Interpreters/Context.h>
#include <pybind11/pybind11.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <Client/LocalConnection.h>

namespace py = pybind11;

std::string hello()
{
    auto sctx  = DB::Context::createShared();
    auto gctx = DB::Context::createGlobal(sctx.get());
    DB::LocalConnection lc(gctx);
    std::string res;
    if (lc.checkConnected())
    {
        res += "yes";
    }
    else
    {
        res += "no";
    }
    DB::ConnectionTimeouts ts;
    lc.sendQuery(ts, "SELECT 1", "", DB::QueryProcessingStage::Complete, nullptr, nullptr, false);
    return res;
    // return toString(DB::ServerUUID::get());
}

PYBIND11_MODULE(mylib, m) {
    m.doc() = "pybind11 mylib plugin";
    m.def("hello", &hello, "hello");
    // m.def("executeMultiQuery", &DB::LocalServer::main, "A function that adds two numbers");
}
