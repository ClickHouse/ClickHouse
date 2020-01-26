/**
 *  Address.h
 *
 *  An AMQP address in the "amqp://user:password@hostname:port/vhost" notation
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */
 
/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class Address
{
private:
    /**
     *  The auth method
     *  @var bool
     */
    bool _secure = false;
    
    /**
     *  Login data (username + password)
     *  @var Login
     */
    Login _login;

    /**
     *  The hostname
     *  @var std::string
     */
    std::string _hostname;

    /**
     *  Port number
     *  @var uint16_t
     */
    uint16_t _port = 5672;

    /**
     *  The vhost
     *  @var std::string
     */
    std::string _vhost;
    
    
    /**
     *  The default port
     *  @return uint16_t
     */
    uint16_t defaultport() const
    {
        return _secure ? 5671 : 5672;
    }

public:
    /**
     *  Constructor to parse an address string
     *  The address should start with "amqp://
     *  @param  data
     *  @param  size
     *  @throws std::runtime_error
     */
    Address(const char *data, size_t size) : _vhost("/")
    {
        // position of the last byte
        const char *last = data + size;

        // must start with ampqs:// to have a secure connection (and we also assign a different default port)
        if (strncmp(data, "amqps://", 8) == 0) _secure = true;

        // otherwise protocol must be amqp://
        else if (strncmp(data, "amqp://", 7) != 0) throw std::runtime_error("AMQP address should start with \"amqp://\" or \"amqps://\"");
        
        // assign default port (we may overwrite it later)
        _port = defaultport();

        // begin of the string was parsed
        data += _secure ? 8 : 7;

        // do we have a '@' to split user-data and hostname?
        const char *at = (const char *)memchr(data, '@', last - data);

        // do we have one?
        if (at != nullptr)
        {
            // size of the user:password
            size_t loginsize = at - data;

            // colon could split username and password
            const char *colon = (const char *)memchr(data, ':', loginsize);

            // assign the login
            _login = Login(
                std::string(data, colon ? colon - data : loginsize),
                std::string(colon ? colon + 1 : "", colon ? at - colon - 1 : 0)
            );

            // set data to the start of the hostname
            data = at + 1;
        }

        // find out where the vhost is set (starts with a slash)
        const char *slash = (const char *)memchr(data, '/', last - data);

        // was a vhost set?
        if (slash != nullptr && last - slash > 1) _vhost.assign(slash + 1, last - slash - 1);

        // the hostname is everything until the slash, check is portnumber was set
        const char *colon = (const char *)memchr(data, ':', last - data);

        // was a portnumber specified (colon must appear before the slash of the vhost)
        if (colon && (!slash || colon < slash))
        {
            // a portnumber was set to
            _hostname.assign(data, colon - data);

            // calculate the port
            _port = atoi(std::string(colon + 1, slash ? slash - colon - 1 : last - colon - 1).data());
        }
        else
        {
            // no portnumber was set
            _hostname.assign(data, slash ? slash - data : last - data);
        }
    }

    /**
     *  Constructor to parse an address string
     *  The address should start with amqp:// or amqps://
     *  @param  data
     *  @throws std::runtime_error
     */
    Address(const char *data) : Address(data, strlen(data)) {}

    /**
     *  Constructor based on std::string
     *  @param  address
     */
    Address(const std::string &address) : Address(address.data(), address.size()) {}

    /**
     *  Constructor based on already known properties
     *  @param  host
     *  @param  port
     *  @param  login
     *  @param  vhost
     *  @param  secure
     */
    Address(std::string host, uint16_t port, Login login, std::string vhost, bool secure = false) :
        _secure(secure),
        _login(std::move(login)),
        _hostname(std::move(host)),
        _port(port),
        _vhost(std::move(vhost)) {}

    /**
     *  Destructor
     */
    virtual ~Address() = default;

    /**
     *  Should we open a secure connection?
     *  @return bool
     */
    bool secure() const
    {
        return _secure;
    }

    /**
     *  Expose the login data
     *  @return Login
     */
    const Login &login() const
    {
        return _login;
    }

    /**
     *  Host name
     *  @return std::string
     */
    const std::string &hostname() const
    {
        return _hostname;
    }

    /**
     *  Port number
     *  @return uint16_t
     */
    uint16_t port() const
    {
        return _port;
    }

    /**
     *  The vhost to connect to
     *  @return std::string
     */
    const std::string &vhost() const
    {
        return _vhost;
    }

    /**
     *  Cast to a string
     *  @return std::string
     */
    operator std::string () const
    {
        // result object
        std::string str(_secure ? "amqps://" : "amqp://");

        // append login
        str.append(_login.user()).append(":").append(_login.password()).append("@").append(_hostname);

        // do we need a special portnumber?
        if (_port != 5672) str.append(":").append(std::to_string(_port));

        // append default vhost
        str.append("/");

        // do we have a special vhost?
        if (_vhost != "/") str.append(_vhost);

        // done
        return str;
    }

    /**
     *  Comparison operator
     *  @param  that
     *  @return bool
     */
    bool operator==(const Address &that) const
    {
        // security setting should match
        if (_secure != that._secure) return false;

        // logins must match
        if (_login != that._login) return false;

        // hostname must match, but are not case sensitive
        if (strcasecmp(_hostname.data(), that._hostname.data()) != 0) return false;

        // portnumber must match
        if (_port != that._port) return false;

        // and finally the vhosts, they must match too
        return _vhost == that._vhost;
    }

    /**
     *  Comparison operator
     *  @param  that
     *  @return bool
     */
    bool operator!=(const Address &that) const
    {
        // the opposite of operator==
        return !operator==(that);
    }
    
    /**
     *  Comparison operator that is useful if addresses have to be ordered
     *  @param  that
     *  @return bool
     */
    bool operator<(const Address &that) const
    {
        // compare auth methods (amqp comes before amqps)
        if (_secure != that._secure) return !_secure;
        
        // compare logins
        if (_login != that._login) return _login < that._login;
        
        // hostname must match, but are not case sensitive
        int result = strcasecmp(_hostname.data(), that._hostname.data());
        
        // if hostnames are not equal, we know the result
        if (result != 0) return result < 0;

        // portnumber must match
        if (_port != that._port) return _port < that._port;

        // and finally compare the vhosts
        return _vhost < that._vhost;
    }
    
    /**
     *  Friend function to allow writing the address to a stream
     *  @param  stream
     *  @param  address
     *  @return std::ostream
     */
    friend std::ostream &operator<<(std::ostream &stream, const Address &address)
    {
        // start with the protocol and login
        stream << (address._secure ? "amqps://" : "amqp://");
        
        // do we have a login?
        if (address._login) stream << address._login << "@";
        
        // write hostname
        stream << address._hostname;

        // do we need a special portnumber?
        if (address._port != address.defaultport()) stream << ":" << address._port;

        // append default vhost
        stream << "/";

        // do we have a special vhost?
        if (address._vhost != "/") stream << address._vhost;

        // done
        return stream;
    }
};

/**
 *  End of namespace
 */
}

