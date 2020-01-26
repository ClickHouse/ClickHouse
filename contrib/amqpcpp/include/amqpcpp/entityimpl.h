/**
 *  EntityImpl.h
 *
 *  Common base class for exchanges and queues.
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class EntityImpl
{
protected:
    /**
     *  The channel on which we communicate
     *  @var    Channel
     */
    Channel *_channel;
    
    /**
     *  Name of the queue/exchange
     *  @var    string
     */
    std::string _name;
    
    /**
     *  Is this a durable queue/exchange?
     *  A durable queue/exchange survives a broker restart
     *  @var bool
     */
    bool _durable = false;
    
    /**
     *  Is this a passive queue/exchange?
     *  If set, only check if the queue/exchange exists without actually creating it
     *  @var bool
     */
    bool _passive = false;
    
    /**
     *  Is this an auto-delete queue/exchange?
     *  If set, the entity is removed when it is no longer used (for queues
     *  when all consumers are gone, for exchanges when all queues are gone)
     *  @var bool
     */
    bool _autoDelete = false;

    /**
     *  Additional arguments
     *  @var Table
     */
    Table _arguments;


    /**
     *  Constructor is protected and can only be accessed by derived classes
     *  @param  channel
     */
    EntityImpl(Channel *channel) : _channel(channel) {}
    
public:
    /**
     *  Destructor
     */
    virtual ~EntityImpl();
    
    /**
     *  Retrieve the name
     *  @return string
     */
    std::string &name()
    {
        return _name;
    }
    
    /**
     *  Change the name
     *  You must declare the entity before this has effect
     *  @param  string
     */
    void setName(const std::string &name)
    {
        _name = name;
    }
    
    /**
     *  Is the queue or exchange durable
     *  The entity survives a broker restart if it is durable
     *  @return bool
     */
    bool durable()
    {
        return _durable;
    }
    
    /**
     *  Mark the object as durable
     *  @param  bool
     */
    void setDurable(bool durable)
    {
        _durable = durable;
    }

    /**
     *  Is the passive bit set
     *  If set, the declare method only checks if the queue/exchange exists without actually creating it
     *  @return bool
     */
    bool passive()
    {
        return _passive;
    }
    
    /**
     *  Change the passive bit
     *  @param  bool
     */
    void setPassive(bool passive)
    {
        _passive = passive;
    }
    
    /**
     *  Is the auto-delete property set?
     *  The entity is removed when the consumers and/or queues are unlinked from it
     *  @return bool
     */
    bool autoDelete()
    {
        return _autoDelete;
    }
    
    /**
     *  Update the auto-delete bit
     *  @param  bool
     */
    void setAutoDelete(bool autoDelete)
    {
        _autoDelete = autoDelete;
    }

    /**
     *  Set a custom argument
     *  @param  name        Name of the argument
     *  @param  value       Value of the argument
     */
    void setArgument(const std::string &name, const std::string &value)
    {
        _arguments[name] = value;
    }
    
    /**
     *  Retrieve a argument
     *  @param  name        Name of the argument
     *  @return             The value of the argument
     */
    std::string &argument(const std::string &name)
    {
        return _arguments[name];
    }
    
    /**
     *  Bind to a different exchange
     *  @param  exchange    The exchange to bind to
     *  @param  key         The routing key
     *  @return bool
     */
    virtual bool bind(const std::string &exchange, const std::string &key) = 0;
    
    /**
     *  Unbind from an exchange
     *  @param  exchange    Exchange to unbind from
     *  @param  key         The routing key
     *  @return bool
     */
    virtual bool unbind(const std::string &exchange, const std::string &key) = 0;
    
    /**
     *  Declare the queue/exchange
     *  @return bool
     */
    virtual bool declare() = 0;
    
    /**
     *  Remove the queue/exchange
     *  @return bool
     */
    virtual bool remove() = 0;
    
};

/**
 *  End of namespace
 */
}

