.. _configuration_files:

Configuration files
======================

The main server config file is ``config.xml``. It resides in the ``/etc/clickhouse-server/`` directory.

Certain settings can be overridden in the ``*.xml`` and ``*.conf`` files from the ``conf.d`` and ``config.d`` directories next to the config.

The ``replace`` and ``remove`` attributes can be specified for the elements of these config files.
If neither is specified, it combines the contents of elements recursively, replacing values of duplicate children.
If ``replace`` is specified, it replaces the entire element with the specified one.
If ``remove`` is specified, it deletes the element.

The config can also define "substitutions". If an element has the ``incl`` attribute, the corresponding substitution from the file will be used as the value. By default, the path to the file with substitutions is ``/etc/metrika.xml``. This can be changed in the config in the ``include_from`` element. The substitution values are specified in  ``/yandex/substitution_name`` elements of this file.

You can also perform substitutions from ZooKeeper nodes. To do that add the ``from_zk="/path/to/node"`` attribute to a config element. Element contents will be substituted with the contents of the /path/to/node ZooKeeper node. The ZooKeeper node can contain a whole XML subtree, and it will be inserted as a child of the substituted node.

The 'config.xml' file can specify a separate config with user settings, profiles, and quotas. The relative path to this config is set in the 'users_config' element. By default, it is 'users.xml'. If 'users_config' is omitted, the user settings, profiles, and quotas are specified directly in ``config.xml``. For ``users_config``, overrides and substitutions may also exist in files from the ``users_config.d`` directory (for example, ``users.d``).

For each config file, the server also generates ``file-preprocessed.xml`` files on launch. These files contain all the completed substitutions and overrides, and they are intended for informational use. If ZooKeeper substitutions were used in a config file and the ZooKeeper is unavailable during server startup, the configuration is loaded from the respective preprocessed file.

The server tracks changes to config files and files and ZooKeeper nodes that were used for substitutions and overrides and reloads users and clusters configurations in runtime. That is, you can add or change users, clusters and their settings without relaunching the server.
