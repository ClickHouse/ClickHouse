# 配置文件 {#configuration_files}

ClickHouse支持多配置文件管理。主配置文件是`/etc/clickhouse-server/config.xml`。其余文件须在目录`/etc/clickhouse-server/config.d`。

!!! 注意：
    所有配置文件必须是XML格式。此外，配置文件须有相同的跟元素，通常是`<yandex>`。

主配置文件中的一些配置可以通过`replace`或`remove`属性被配置文件覆盖。

如果两者都未指定，则递归组合配置的内容，替换重复子项的值。

如果指定`replace`属性，则将整个元素替换为指定的元素。

如果指定`remove`属性，则删除该元素。

此外，配置文件还可指定"substitutions"。如果一个元素有`incl`属性，则文件中的相应替换值将被使用。默认情况下，具有替换的文件的路径为`/etc/metrika.xml`。这可以在服务配置中的[include\_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from)元素中被修改。替换值在这个文件的`/yandex/substitution_name`元素中被指定。如果`incl`中指定的替换值不存在，则将其记录在日志中。为防止ClickHouse记录丢失的替换，请指定`optional="true"`属性（例如，[宏](server-configuration-parameters/settings.md)设置）。

替换也可以从ZooKeeper执行。为此，请指定属性`from_zk = "/path/to/node"`。元素值被替换为ZooKeeper节点`/path/to/node`的内容。您还可以将整个XML子树放在ZooKeeper节点上，并将其完全插入到源元素中。

`config.xml` 文件可以指定单独的配置文件用于配置用户设置、配置文件及配额。可在`users_config`元素中指定其配置文件相对路径。其默认值是`users.xml`。如果`users_config`被省略，用户设置，配置文件和配额则直接在`config.xml`中指定。

用户配置可以分为如`config.xml`和`config.d/`等形式的单独配置文件。目录名称为配置`user_config`的值，去掉`.xml`后缀并与添加`.d`。由于`users_config`配置默认值为`users.xml`，所以目录名默认使用`users.d`。例如，您可以为每个用户有单独的配置文件，如下所示：

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
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

对于每个配置文件，服务器还会在启动时生成 `file-preprocessed.xml` 文件。这些文件包含所有已完成的替换和复盖，并且它们旨在提供信息。如果zookeeper替换在配置文件中使用，但ZooKeeper在服务器启动时不可用，则服务器将从预处理的文件中加载配置。

服务器跟踪配置文件中的更改，以及执行替换和复盖时使用的文件和ZooKeeper节点，并动态重新加载用户和集群的设置。 这意味着您可以在不重新启动服务器的情况下修改群集、用户及其设置。

[原始文章](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
