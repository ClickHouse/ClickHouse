#include <iostream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <ThriftHiveMetastore.h>


using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;

int main()
{
    std::shared_ptr<TTransport> socket(new TSocket("localhost", 9083));
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ThriftHiveMetastoreClient client(protocol);

    try
    {
        transport->open();

        Table table;
        client.get_table(table, "default", "persons");
        table.printTo(std::cout);

        vector<Partition> partitions;
        client.get_partitions(partitions, "default", "persons", 0);
        for (const auto & part : partitions)
        {
            part.printTo(std::cout);
        }

        transport->close();
    }
    catch (TException & tx)
    {
        cout << "ERROR: " << tx.what() << endl;
    }
}
