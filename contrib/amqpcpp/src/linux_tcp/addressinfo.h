/**
 *  AddressInfo.h
 *
 *  Utility wrapper arround "getAddressInfo()"
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 Copernica BV
 */

/**
 *  Include guard
 */
namespace AMQP {

/**
 *  Class definition
 */
class AddressInfo
{
private:
    /**
     *  The addresses
     *  @var struct AddressInfo
     */
    struct addrinfo *_info = nullptr;
    
    /**
     *  Vector of addrinfo pointers
     *  @var std::vector<struct addrinfo *>
     */
    std::vector<struct addrinfo *> _v;

public:
    /**
     *  Constructor
     *  @param  hostname
     *  @param  port
     */
    AddressInfo(const char *hostname, uint16_t port = 5672)
    {
        // store portnumber in buffer
        auto portnumber = std::to_string(port);
        
        // info about the lookup
        struct addrinfo hints;
        
        // set everything to zero
        memset(&hints, 0, sizeof(struct addrinfo));
        
        // set hints
        hints.ai_family = AF_UNSPEC;        // allow IPv4 or IPv6
        hints.ai_socktype = SOCK_STREAM;    // datagram socket/
        
        // get address of the server
        auto code = getaddrinfo(hostname, portnumber.data(), &hints, &_info);
        
        // was there an error
        if (code != 0) throw std::runtime_error(gai_strerror(code));
        
        // keep looping
        for (auto *current = _info; current; current = current->ai_next)
        {
            // store in vector
            _v.push_back(current);
        }
    }

    /**
     *  Destructor
     */
    virtual ~AddressInfo()
    {
        // free address info
        freeaddrinfo(_info);
    }
    
    /**
     *  Size of the array
     *  @return size_t
     */
    size_t size() const
    {
        return _v.size();
    }
    
    /**
     *  Get reference to struct
     *  @param  index
     *  @return struct addrinfo*
     */
    const struct addrinfo *operator[](int index) const
    {
        // expose vector
        return _v[index];
    }
};

/**
 *  End of namespace
 */
}

