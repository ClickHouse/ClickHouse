/**
 *  Addresses.h
 *  
 *  Class that contains multiple addresses
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2017 Copernica BV
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
class Addresses
{
private:
    /**
     *  The actual addresses
     *  @var std::vector<Address>
     */
    std::vector<Address> _addresses;

public:
    /**
     *  Constructor for a comma separated list
     *  @param  buffer
     *  @param  size
     *  @throws std::runtime_error
     */
    Addresses(const char *buffer, size_t size)
    {
        // keep looping
        while (true)
        {
            // look for the comma
            const char *comma = memchr(buffer, ',', size);
            
            // stop if there is no comma
            if (comma == nullptr) break;
            
            // size of the address
            size_t addresssize = comma - buffer - 1;
            
            // add address
            _addresses.emplace_back(buffer, addresssize);
            
            // update for next iteration
            buffer += addresssize + 1;
            size -= addresssize + 1;
        }
        
        // do we have more?
        if (size > 0) _addresses.emplace_back(buffer, size);
        
        // was anything found?
        if (_addresses.size() > 0) return;
        
        // no addresses found
        throw std::runtime_error("no addresses");
    }

    /**
     *  Destructed
     */
    virtual ~Addresses() = default;
    
    /**
     *  Number of addresses
     *  @return size_t
     */
    size_t size() const
    {
        return _addresses.size();
    }
    
    /**
     *  Expose an address by index
     *  @param  index
     *  @return Address
     */
    const Address &operator[](size_t index) const
    {
        return _addresses.at(index);
    }
};

/**
 *  End of namespace
 */
}

