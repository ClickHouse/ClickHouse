#ifndef CLICKHOUSE_FDSTREAM_H
#define CLICKHOUSE_FDSTREAM_H

#include <array>
#include <streambuf>
#include <iostream>
#include <unistd.h>

constexpr auto BUF_SIZE = 1024u;

class FDBuf : public std::streambuf {
public:
    FDBuf () : fd_(-1), need_close(false) { }
    explicit FDBuf (int fd) : fd_(fd) { }

    FDBuf(FDBuf &&other) : fd_(other.fd_) {
        other.dismiss();
        other.fd_ = -1;
        other.in_cur_ = other.in_size_ = other.out_size_ = 0;
    }

    ~FDBuf() {
        if (need_close) {
            close(fd_);
        }
    }

    int dismiss() {
        need_close = false;
        return fd_;
    }

    int get_fd() const {
        return fd_;
    }

    void set_fd(int fd) {
        if (need_close) {
            close(fd_);
        }
        fd_ = fd;
    }

protected:
    int overflow (int c) final {
        throw std::runtime_error("Not implemented");
    }

    int sync() {
        if (out_size_== 0) {
            return 0;
        }
        int res;
        do {
            errno = 0;
            res = write(fd_, out_buf_.data(), out_size_);
        } while (res == -1 && errno == EINTR);
        if (res != out_size_) {
            throw std::runtime_error("Can not write to fd");
        }
        out_size_ = 0;
        return 0;
    }

    std::streamsize xsputn(const char* s, std::streamsize num_) final {
        unsigned long num = num_;
        while (num > 0) {
            auto can_write = std::min(out_buf_.size() - out_size_, num);
            if (can_write > 0) {
                memcpy(out_buf_.data() + out_size_, s, can_write);
                s += can_write;
                num -= can_write;
                out_size_ += can_write;
            }
            if (static_cast<unsigned>(out_size_) == out_buf_.size()) {
                int res;
                do {
                    errno = 0;
                    res = write(fd_, out_buf_.data(), out_size_);
                } while (res == -1 && errno == EINTR);
                if (res != out_size_) {
                    throw std::runtime_error("Can not write to fd");
                }
                out_size_ = 0;
            }
        }
        return num_;
    }

    std::streamsize xsgetn(char* s, std::streamsize num_) final {
        int num = num_;
        while (num > 0) {
            if (in_cur_ < in_size_) {
                auto can_read = std::min(in_size_ - in_cur_, num);
                if (can_read > 0) {
                    memcpy(s, in_buf_.data() + in_cur_, can_read);
                    s += can_read;
                    num -= can_read;
                    in_cur_ += can_read;
                }
            } else {
                do {
                    errno = 0;
                    in_size_ = read(fd_, in_buf_.data(), in_buf_.size());
                } while (in_size_ == -1 && errno == EINTR);
                in_cur_ = 0;
                if (in_size_ == -1) {
                    throw std::runtime_error("Can not read from fd");
                }
                if (in_size_ == 0) {
                    /// EOF
                    return num_;
                }
            }
        }
        return num_;
    }

    int underflow() final {
        if (in_size_ == -1) {
            return EOF;
        }
        if (in_cur_ != in_size_) {
            return in_buf_[in_cur_];
        }
        do {
            errno = 0;
            in_size_ = read(fd_, in_buf_.data(), in_buf_.size());
        } while (in_size_ == -1 && errno == EINTR);
        in_cur_ = 0;
        if (in_size_ == -1) {
            throw std::runtime_error("Can not read from fd");
        }
        if (in_size_ == 0) {
            return EOF;
        }
        return in_buf_[in_cur_];
    }

    int uflow() final {
        if (in_size_ == -1) {
            return EOF;
        }
        if (in_cur_ != in_size_) {
            return in_buf_[in_cur_++];
        }
        do {
            errno = 0;
            in_size_ = read(fd_, in_buf_.data(), in_buf_.size());
        } while (in_size_ == -1 && errno == EINTR);
        in_cur_ = 0;
        if (in_size_ == -1) {
            throw std::runtime_error("Can not read from fd");
        }
        if (in_size_ == 0) {
            return EOF;
        }
        return in_buf_[in_cur_++];
    }

    std::streamsize showmanyc() final {
        throw std::runtime_error("Not implemented");
    }

    int pbackfail(int c) final {
        throw std::runtime_error("Not implemented");
    }

private:
    int fd_;
    bool need_close = true;
    int in_cur_ = 0, in_size_ = 0, out_size_ = 0;

    std::array<unsigned char, BUF_SIZE> in_buf_{};
    std::array<char, BUF_SIZE> out_buf_{};
};

class FDOStream : public std::ostream {
protected:
    FDBuf buf_;
public:
    explicit FDOStream(int fd) : std::ostream(nullptr), buf_(fd) {
        rdbuf(&buf_);
    }

    ~FDOStream() {
        flush();
    }

    int dismiss() {
        return buf_.dismiss();
    }

    int get_fd() const {
        return buf_.get_fd();
    }

    void set_fd(int fd) {
        buf_.set_fd(fd);
    }
};

class FDIStream : public std::istream {
protected:
    FDBuf buf_;
public:
    explicit FDIStream(int fd) : std::istream(nullptr), buf_(fd) {
        rdbuf(&buf_);
    }

    int dismiss() {
        return buf_.dismiss();
    }

    int get_fd() const {
        return buf_.get_fd();
    }

    void set_fd(int fd) {
        buf_.set_fd(fd);
    }
};

class FDIOStream : public std::iostream {
protected:
    FDBuf buf_;
public:
    explicit FDIOStream(int fd) : std::iostream(nullptr), buf_(fd) {
        rdbuf(&buf_);
    }

    ~FDIOStream() {
        flush();
    }

    int dismiss() {
        return buf_.dismiss();
    }

    int get_fd() const {
        return buf_.get_fd();
    }
};

#endif //CLICKHOUSE_FDSTREAM_H
