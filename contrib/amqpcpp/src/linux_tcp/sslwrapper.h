/**
 *  SslWrapper.h
 *
 *  Wrapper around a SSL pointer
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class SslWrapper
{
private:
    /**
     *  The wrapped object
     *  @var SSL*
     */
    SSL *_ssl;
    
public:
    /**
     *  Constructor
     *  @param  ctx
     *  @param  file
     */
    SslWrapper(SSL_CTX *ctx) : _ssl(OpenSSL::SSL_new(ctx)) 
    {
        // report error
        if (_ssl == nullptr) throw std::runtime_error("failed to construct ssl structure");
        
        //OpenSSL::SSL_use_certificate_file(_ssl, "cert.pem", SSL_FILETYPE_PEM);
    }
    
    /**
     *  Copy constructor is removed because openssl 1.0 has no way to up refcount 
     *  (otherwise we could be safely copying objects around)
     *  @param  that
     */
    SslWrapper(const SslWrapper &that) = delete;
    
    /**
     *  Move constructor
     *  @param  that
     */
    SslWrapper(SslWrapper &&that) : _ssl(that._ssl)
    {
        // invalidate other object
        that._ssl = nullptr;
    }
    
    /**
     *  Destructor
     */
    virtual ~SslWrapper()
    {
        // do nothing if already moved away
        if (_ssl == nullptr) return;
        
        // destruct object
        OpenSSL::SSL_free(_ssl);
    }
    
    /**
     *  Cast to the SSL*
     *  @return SSL *
     */
    operator SSL * () const { return _ssl; }
};

/**
 *  End of namespace
 */
}

