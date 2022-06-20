---
sidebar_position: 45
sidebar_label: Secured Communication with Zookeeper
---

# Optional secured communication between ClickHouse and Zookeeper

You should specify `ssl.keyStore.location`, `ssl.keyStore.password` and `ssl.trustStore.location`, `ssl.trustStore.password` for communication with ClickHouse client over SSL. These options are available from Zookeeper version 3.5.2.

You can add `zookeeper.crt` to trusted certificates.

``` bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

Client section in `config.xml` will look like:

``` xml
<client>
    <certificateFile>/etc/clickhouse-server/client.crt</certificateFile>
    <privateKeyFile>/etc/clickhouse-server/client.key</privateKeyFile>
    <loadDefaultCAFile>true</loadDefaultCAFile>
    <cacheSessions>true</cacheSessions>
    <disableProtocols>sslv2,sslv3</disableProtocols>
    <preferServerCiphers>true</preferServerCiphers>
    <invalidCertificateHandler>
        <name>RejectCertificateHandler</name>
    </invalidCertificateHandler>
</client>
```

Add Zookeeper to ClickHouse config with some cluster and macros:

``` xml
<clickhouse>
    <zookeeper>
        <node>
            <host>localhost</host>
            <port>2281</port>
            <secure>1</secure>
        </node>
    </zookeeper>
</clickhouse>
```

Start `clickhouse-server`. In logs you should see:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

Prefix `secure://` indicates that connection is secured by SSL.

To ensure traffic is encrypted run `tcpdump` on secured port:

```bash
tcpdump -i any dst port 2281 -nnXS
```

And query in `clickhouse-client`:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

On unencrypted connection you will see in `tcpdump` output something like this:

```text
..../zookeeper/quota.
```

On encrypted connection you should not see this.
