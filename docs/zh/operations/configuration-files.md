# 配置文件 {#configuration_files}

主服务器配置文件是 `config.xml`. 它驻留在 `/etc/clickhouse-server/` 目录。

单个设置可以在复盖 `*.xml` 和 `*.conf` 在文件 `conf.d` 和 `config.d` 配置文件旁边的目录。

该 `replace` 或 `remove` 可以为这些配置文件的元素指定属性。

如果两者都未指定，则递归组合元素的内容，替换重复子项的值。

如果 `replace` 如果指定，则将整个元素替换为指定的元素。

如果 `remove` 如果指定，则删除该元素。

The config can also define «substitutions». If an element has the `incl` 属性时，从文件中的相应替换将被用作该值。 默认情况下，具有替换的文件的路径为 `/etc/metrika.xml`. 这可以在改变 [包括\_从](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) 服务器配置中的元素。 替换值在指定 `/yandex/substitution_name` 这个文件中的元素。 如果在指定的替换 `incl` 不存在，则将其记录在日志中。 要防止ClickHouse记录丢失的替换，请指定 `optional="true"` 属性（例如，设置 [宏](#macros) server\_settings/settings.md))。

替换也可以从ZooKeeper执行。 为此，请指定属性 `from_zk = "/path/to/node"`. 元素值被替换为节点的内容 `/path/to/node` 在动物园管理员。 您还可以将整个XML子树放在ZooKeeper节点上，并将其完全插入到源元素中。

该 `config.xml` 文件可以指定具有用户设置、配置文件和配额的单独配置。 这个配置的相对路径在 ‘users\_config’ 元素。 默认情况下，它是 `users.xml`. 如果 `users_config` 被省略，用户设置，配置文件和配额直接在指定 `config.xml`.

此外, `users_config` 可以从文件中复盖 `users_config.d` 目录（例如, `users.d`）和替换。 例如，您可以为每个用户提供单独的配置文件，如下所示:

``` xml
$ cat /etc/clickhouse-server/users.d/alice.xml
<yandex>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</yandex>
```

对于每个配置文件，服务器还会生成 `file-preprocessed.xml` 启动时的文件。 这些文件包含所有已完成的替换和复盖，并且它们旨在提供信息。 如果zookeeper替换在配置文件中使用，但ZooKeeper在服务器启动时不可用，则服务器将从预处理的文件中加载配置。

服务器跟踪配置文件中的更改，以及执行替换和复盖时使用的文件和ZooKeeper节点，并动态重新加载用户和集群的设置。 这意味着您可以在不重新启动服务器的情况下修改群集、用户及其设置。

[原始文章](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
