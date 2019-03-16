#include <IO/WriteBuffer.h>
#include <random>
#include <sstream>

/// Implementation of MySQL wire protocol

namespace DB {
namespace MySQLProtocol {

uint64_t readLenenc(std::istringstream &ss) {
    char c;
    uint64_t buf = 0;
    ss.get(c);
    auto cc = static_cast<uint8_t>(c);
    if (cc < 0xfc) {
        return cc;
    } else if (cc < 0xfd) {
        ss.read(reinterpret_cast<char *>(&buf), 2);
    } else if (cc < 0xfe) {
        ss.read(reinterpret_cast<char *>(&buf), 3);
    } else {
        ss.read(reinterpret_cast<char *>(&buf), 8);
    }
    return buf;
}

std::string writeLenenc(uint64_t x) {
    std::string result;
    if (x < 251) {
        result.append(1, static_cast<char>(x));
    } else if (x < (1 << 16)) {
        result.append(1, 0xfc);
        result.append(reinterpret_cast<char *>(&x), 2);
    } else if (x < (1 << 24)) {
        result.append(1, 0xfd);
        result.append(reinterpret_cast<char *>(&x), 3);
    } else {
        result.append(1, 0xfe);
        result.append(reinterpret_cast<char *>(&x), 8);
    }
    return result;
}

void writeLenencStr(std::string &payload, const std::string &s) {
    payload.append(writeLenenc(s.length()));
    payload.append(s);
}

}
}
