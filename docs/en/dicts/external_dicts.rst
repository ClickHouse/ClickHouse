External dictionaries
===============

It is possible to add your own dictionaries from various data sources. The data source for a dictionary can be a file in the local file system, the ClickHouse server, or a MySQL server.
A dictionary can be stored completely in RAM and updated regularly, or it can be partially cached in RAM and dynamically load missing values.

The configuration of external dictionaries is in a separate file or files specified in the 'dictionaries_config' configuration parameter.
This parameter contains the absolute or relative path to the file with the dictionary configuration. A relative path is relative to the directory with the server config file. The path can contain wildcards * and ?, in which case all matching files are found. Example: dictionaries/*.xml.

The dictionary configuration, as well as the set of files with the configuration, can be updated without restarting the server. The server checks updates every 5 seconds. This means that dictionaries can be enabled dynamically.

Dictionaries can be created when starting the server, or at first use. This is defined by the 'dictionaries_lazy_load' parameter in the main server config file. This parameter is optional, 'true' by default. If set to 'true', each dictionary is created at first use. If dictionary creation failed, the function that was using the dictionary throws an exception. If 'false', all dictionaries are created when the server starts, and if there is an error, the server shuts down.

The dictionary config file has the following format:

.. code-block:: xml

  <dictionaries>
      <comment>Optional element with any content; completely ignored.</comment>

      <!--You can set any number of different dictionaries. -->
      <dictionary>
          <!-- Dictionary name. The dictionary will be accessed for use by this name. -->
          <name>os</name>
  
          <!-- Data source. -->
          <source>
              <!-- Source is a file in the local file system. -->
              <file>
                  <!-- Path on the local file system. -->
                  <path>/opt/dictionaries/os.tsv</path>
                  <!-- Which format to use for reading the file. -->
                  <format>TabSeparated</format>
              </file>

              <!-- or the source is a table on a MySQL server.
              <mysql>
                  <!- - These parameters can be specified outside (common for all replicas) or inside a specific replica - ->
                  <port>3306</port>
                  <user>clickhouse</user>
                  <password>qwerty</password>
                  <!- - Specify from one to any number of replicas for fault tolerance. - ->
                  <replica>
                      <host>example01-1</host>
                      <priority>1</priority> <!- - The lower the value, the higher the priority. - ->
                  </replica>
                  <replica>
                      <host>example01-2</host>
                      <priority>1</priority>
                  </replica>
                  <db>conv_main</db>
                  <table>counters</table>
              </mysql>
              -->

              <!-- or the source is a table on the ClickHouse server.
              <clickhouse>
                  <host>example01-01-1</host>
                  <port>9000</port>
                  <user>default</user>
                  <password></password>
                  <db>default</db>
                  <table>counters</table>
              </clickhouse>
              <!- - If the address is similar to localhost, the request is made without network interaction. For fault tolerance, you can create a Distributed table on localhost and enter it. - ->
              -->

              <!-- or the source is a executable. If layout.complex_key_cache - list of needed keys will be written in STDIN of program -->
              <executable>
                  <!-- Path on the local file system or name located in one of env PATH dirs. -->
                  <command>cat /opt/dictionaries/os.tsv</command>
                  <!-- Which format to use for reading/writing stream. -->
                  <format>TabSeparated</format>
              </executable>

              <!-- or the source is a http server. If layout.complex_key_cache - list of needed keys will be sent as POST  -->
              <http>
                  <!-- Host. -->
                  <url>http://[::1]/os.tsv</url>
                  <!-- Which format to use for reading answer and making POST. -->
                  <format>TabSeparated</format>
              </http>

          </source>

          <!-- Update interval for fully loaded dictionaries. 0 - never update. -->
          <lifetime>
              <min>300</min>
              <max>360</max>
              <!-- The update interval is selected uniformly randomly between min and max, in order to spread out the load when updating dictionaries on a large number of servers. -->
          </lifetime>

          <!-- or <!- - The update interval for fully loaded dictionaries or invalidation time for cached dictionaries. 0 - never update. - ->
          <lifetime>300</lifetime>
          -->

          <layout> <!-- Method for storing in memory. -->
              <flat />
              <!-- or <hashed />
              or
              <cache>
                  <!- - Cache size in number of cells; rounded up to a degree of two. - ->
                  <size_in_cells>1000000000</size_in_cells>
              </cache>
              or
              <ip_trie />
              -->
          </layout>

          <!-- Structure. -->
          <structure>
              <!-- Description of the column that serves as the dictionary identifier (key). -->
              <id>
                  <!-- Column name with ID. -->
                  <name>Id</name>
              </id>

              <attribute>
                  <!-- Column name. -->
                  <name>Name</name>
                  <!-- Column type. (How the column is understood when loading. For MySQL, a table can have TEXT, VARCHAR, and BLOB, but these are all loaded as String) -->
                   <type>String</type>
                  <!-- Value to use for a non-existing element. In the example, an empty string. -->
                  <null_value></null_value>
              </attribute>
              <!-- Any number of attributes can be specified. -->
              <attribute>
                  <name>ParentID</name>
                  <type>UInt64</type>
                  <null_value>0</null_value>
                  <!-- Whether it defines a hierarchy - mapping to the parent ID (by default, false). -->
                  <hierarchical>true</hierarchical>
                  <!-- The mapping id -> attribute can be considered injective, in order to optimize GROUP BY. (by default, false) -->
                  <injective>true</injective>
              </attribute>
          </structure>
      </dictionary>
  </dictionaries>

The dictionary identifier (key attribute) should be a number that fits into UInt64. Also, you can use arbitrary tuples as keys (see section "Dictionaries with complex keys"). Note: you can use complex keys consisting of just one element. This allows using e.g. Strings as dictionary keys.

There are six ways to store dictionaries in memory.

flat
-----
This is the most effective method. It works if all keys are smaller than ``500,000``.  If a larger key is discovered when creating the dictionary, an exception is thrown and the dictionary is not created. The dictionary is loaded to RAM in its entirety. The dictionary uses the amount of memory proportional to maximum key value. With the limit of 500,000, memory consumption is not likely to be high. All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

hashed
-------
This method is slightly less effective than the first one. The dictionary is also loaded to RAM in its entirety, and can contain any number of items with any identifiers. In practice, it makes sense to use up to tens of millions of items, while there is enough RAM.
All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

cache
-------
This is the least effective method. It is appropriate if the dictionary doesn't fit in RAM. It is a cache of a fixed number of cells, where frequently-used data can be located. MySQL, ClickHouse, executable, http sources are supported, but file sources are not supported. 
When searching a dictionary, the cache is searched first. For each data block, all keys not found in the cache (or expired keys) are collected in a package, which is sent to the source with the query ``SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)``. The received data is then written to the cache.

range_hashed
--------
The table lists some data for date ranges, for each key. To give the possibility to extract this data for a given key, for a given date.

Example: in the table there are discounts for each advertiser in the form:
::
  advertiser id    discount start date    end date    value
  123                 2015-01-01                     2015-01-15    0.15
  123                 2015-01-16                     2015-01-31    0.25
  456                 2015-01-01                     2015-01-15    0.05

Adding layout = range_hashed.
When using such a layout, the structure should have the elements range_min, range_max.

Example:

.. code-block:: xml

  <structure>
      <id>
          <name>Id</name>
      </id>
      <range_min>
          <name>first</name>
      </range_min>
      <range_max>
          <name>last</name>
      </range_max>
      ...
      
These columns must be of type Date. Other types are not yet supported.
The columns indicate a closed date range.

To work with such dictionaries, dictGetT functions must take one more argument - the date:

``dictGetT('dict_name', 'attr_name', id, date)``

The function takes out the value for this id and for the date range, which includes the transmitted date. If no id is found or the range found is not found for the found id, the default value for the dictionary is returned.

If there are overlapping ranges, then any suitable one can be used.

If the range boundary is NULL or is an incorrect date (1900-01-01, 2039-01-01), then the range should be considered open. The range can be open on both sides.

In the RAM, the data is presented as a hash table with a value in the form of an ordered array of ranges and their corresponding values.

Example of a dictionary by ranges:

.. code-block:: xml

  <dictionaries>
          <dictionary>
                  <name>xxx</name>
                  <source>
                          <mysql>
                                  <password>xxx</password>
                                  <port>3306</port>
                                  <user>xxx</user>
                                  <replica>
                                          <host>xxx</host>
                                          <priority>1</priority>
                                  </replica>
                                  <db>dicts</db>
                                  <table>xxx</table>
                          </mysql>
                  </source>
                  <lifetime>
                          <min>300</min>
                          <max>360</max>
                  </lifetime>
                  <layout>
                          <range_hashed />
                  </layout>
                  <structure>
                          <id>
                                  <name>Abcdef</name>
                          </id>
                          <range_min>
                                  <name>StartDate</name>
                          </range_min>
                          <range_max>
                                  <name>EndDate</name>
                          </range_max>
                          <attribute>
                                  <name>XXXType</name>
                                  <type>String</type>
                                  <null_value />
                          </attribute>
                  </structure>
          </dictionary>
  </dictionaries>

ip_trie
-------
The table stores IP prefixes for each key (IP address), which makes it possible to map IP addresses to metadata such as ASN or threat score.

Example: in the table there are prefixes matches to AS number and country:
::
  prefix            asn       cca2
  202.79.32.0/20    17501     NP
  2620:0:870::/48   3856      US
  2a02:6b8:1::/48   13238     RU
  2001:db8::/32     65536     ZZ


When using such a layout, the structure should have the "key" element.

Example:

.. code-block:: xml

  <structure>
      <key>
          <attribute>
              <name>prefix</name>
              <type>String</type>
          </attribute>
      </key>
      <attribute>
              <name>asn</name>
              <type>UInt32</type>
              <null_value />
      </attribute>
      <attribute>
              <name>cca2</name>
              <type>String</type>
              <null_value>??</null_value>
      </attribute>
      ...
      
These key must have only one attribute of type String, containing a valid IP prefix. Other types are not yet supported.

For querying, same functions (dictGetT with tuple) as for complex key dictionaries have to be used:

``dictGetT('dict_name', 'attr_name', tuple(ip))``

The function accepts either UInt32 for IPv4 address or FixedString(16) for IPv6 address in wire format:

``dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))``

No other type is supported. The function returns attribute for a prefix matching the given IP address. If there are overlapping prefixes, the most specific one is returned.

The data is stored currently in a bitwise trie, it has to fit in memory.

complex_key_hashed
----------------

The same as ``hashed``, but for complex keys.

complex_key_cache
----------

The same as ``cache``, but for complex keys.

Notes
----------

We recommend using the ``flat`` method when possible, or ``hashed``. The speed of the dictionaries is impeccable with this type of memory storage.

Use the cache method only in cases when it is unavoidable. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary only works normally for high enough hit rates (recommended 99% and higher). You can view the average hit rate in the system.dictionaries table. Set a large enough cache size. You will need to experiment to find the right number of cells - select a value, use a query to get the cache completely full, look at the memory consumption (this information is in the system.dictionaries table), then proportionally increase the number of cells so that a reasonable amount of memory is consumed. We recommend MySQL as the source for the cache, because ClickHouse doesn't handle requests with random reads very well.

In all cases, performance is better if you call the function for working with a dictionary after ``GROUP BY``, and if the attribute being fetched is marked as injective. For a dictionary cache, performance improves if you call the function after LIMIT. To do this, you can use a subquery with LIMIT, and call the function with the dictionary from the outside.

An attribute is called injective if different attribute values correspond to different keys. So when ``GROUP BY`` uses a function that fetches an attribute value by the key, this function is automatically taken out of ``GROUP BY``.

When updating dictionaries from a file, first the file modification time is checked, and it is loaded only if the file has changed.
When updating from MySQL, for flat and hashed dictionaries, first a ``SHOW TABLE STATUS`` query is made, and the table update time is checked. If it is not NULL, it is compared to the stored time. This works for MyISAM tables, but for InnoDB tables the update time is unknown, so loading from InnoDB is performed on each update.

For cache dictionaries, the expiration (lifetime) of data in the cache can be set. If more time than 'lifetime' has passed since loading the data in a cell, the cell's value is not used, and it is re-requested the next time it needs to be used.

If a dictionary couldn't be loaded even once, an attempt to use it throws an exception.
If an error occurred during a request to a cached source, an exception is thrown.
Dictionary updates (other than loading for first use) do not block queries. During updates, the old version of a dictionary is used. If an error occurs during an update, the error is written to the server log, and queries continue using the old version of dictionaries.

You can view the list of external dictionaries and their status in the system.dictionaries table.

To use external dictionaries, see the section "Functions for working with external dictionaries".

Note that you can convert values for a small dictionary by specifying all the contents of the dictionary directly in a ``SELECT`` query (see the section "transform function"). This functionality is not related to external dictionaries.

Dictionaries with complex keys
----------------------------

You can use tuples consisting of fields of arbitrary types as keys. Configure your dictionary with ``complex_key_hashed`` or ``complex_key_cache`` layout in this case.

Key structure is configured not in the ``<id>`` element but in the ``<key>`` element. Fields of the key tuple are configured analogously to dictionary attributes. Example:

.. code-block:: xml

  <structure>
      <key>
          <attribute>
              <name>field1</name>
              <type>String</type>
          </attribute>
          <attribute>
              <name>field2</name>
              <type>UInt32</type>
          </attribute>
          ...
      </key>
  ...


When using such dictionary, use a Tuple of field values as a key in dictGet* functions. Example: ``dictGetString('dict_name', 'attr_name', tuple('field1_value', 123))``.
