#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/CryptographicHashingWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Types.h>

#include <optional>
#include <variant>

namespace DB
{
class AbstractHashingWriteBuffer
{
public:
    using uint128 = std::pair<uint64_t, uint64_t>;

    AbstractHashingWriteBuffer(WriteBuffer & out_, bool cryptographic_mode_, HashFn hashFnType)
        : cryptographic_mode(cryptographic_mode_)
    {
        if (!cryptographic_mode) {
            underlying_buf.emplace<HashingWriteBuffer>(out_);
        } else {
            switch (hashFnType) {
                case HashFn::SipHash:
                    underlying_buf.emplace<CryptoHashingWriteBuffer>(out_);
                    break;
                default:
            }
        }
    }
private:
    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

    struct RefFunc {
        RefFunc(): _unused(nullptr, 0) {}
        WriteBuffer& operator()(HashingWriteBuffer& buf) {
            return buf;
        }
        WriteBuffer& operator()(CryptoHashingWriteBuffer& buf) {
            return buf;
        }
        WriteBuffer& operator()(std::monostate) {
            std::unreachable();
            return _unused;
        }
        WriteBuffer _unused;
    };

public:

    void sync()
    {
        auto functor = overloaded {
            [](std::monostate) {},
            [](auto& buf) {buf.sync();}
        };
        std::visit(functor, underlying_buf);
    }

    uint128 getHash()
    {
        auto functor = overloaded {
            [](std::monostate) {return uint128{};},
            [](auto& buf) {return buf.getHash();}
        };
        return std::visit(functor, underlying_buf);
    }

    void append(DB::BufferBase::Position data)
    {
        auto functor = overloaded {
            [](std::monostate) {},
            [data](auto& buf) {buf.append(data);}
        };
        std::visit(functor, underlying_buf);
    }

    void calculateHash(DB::BufferBase::Position data, size_t len)
    {
        auto functor = overloaded {
            [](std::monostate) {},
            [data, len](auto& buf) {buf.calculateHash(data,len);}
        };
        std::visit(functor, underlying_buf);
    }

    size_t count()
    {
        auto functor = overloaded {
            [](std::monostate) -> size_t {return 0;},
            [](auto& buf) -> size_t {return buf.count();}
        };
        return std::visit(functor, underlying_buf);
    }

    WriteBuffer & getBuf()
    {
        return std::visit(RefFunc{}, underlying_buf);
    }

    inline void next()
    {
        auto functor = overloaded {
            [](std::monostate) {},
            [](auto& buf) {buf.next();}
        };
        std::visit(functor, underlying_buf);
    }

    inline void nextIfAtEnd()
    {
        auto functor = overloaded {
            [](std::monostate) {},
            [](auto& buf) {buf.nextIfAtEnd();}
        };
        std::visit(functor, underlying_buf);
    }

    size_t offset() const
    {
        auto functor = overloaded {
            [](std::monostate) -> size_t {return 0;},
            [](auto& buf) -> size_t {return buf.offset();}
        };
        return std::visit(functor, underlying_buf);
    }

private:
    bool cryptographic_mode;
    std::variant<std::monostate, HashingWriteBuffer, CryptoHashingWriteBuffer> underlying_buf;
};
}
