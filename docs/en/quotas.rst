Quotas
======

Quotas allow you to limit resource usage over a period of time, or simply track the use of resources.
Quotas are set up in the user config. This is usually ``users.xml``.

The system also has a feature for limiting the complexity of a single query (see the section "Restrictions on query complexity").

In contrast to query complexity restrictions, quotas:
 * place restrictions on a set of queries that can be run over a period of time, instead of limiting a single query.
 * account for resources spent on all remote servers for distributed query processing.

Let's look at the section of the ``users.xml`` file that defines quotas.

.. code-block:: xml

  <!-- Quotas. -->
  <quotas>
      <!-- Quota name. -->
      <default>
          <!-- Restrictions for a time period. You can set multiple time intervals with various restrictions. -->
          <interval>
              <!-- Length of time. -->
              <duration>3600</duration>

              <!-- No restrictions. Just collect data for the specified time interval. -->
              <queries>0</queries>
              <errors>0</errors>
              <result_rows>0</result_rows>
              <read_rows>0</read_rows>
              <execution_time>0</execution_time>
          </interval>
      </default>

By default, the quota just tracks resource consumption for each hour, without limiting usage.

.. code-block:: xml

  <statbox>
      <!-- Restrictions for a time period. You can set multiple time intervals with various restrictions. -->
      <interval>
          <!-- Length of time.-->
          <duration>3600</duration>
          <queries>1000</queries>
          <errors>100</errors>
          <result_rows>1000000000</result_rows>
          <read_rows>100000000000</read_rows>
          <execution_time>900</execution_time>
      </interval>
      <interval>
          <duration>86400</duration>
          <queries>10000</queries>
          <errors>1000</errors>
          <result_rows>5000000000</result_rows>
          <read_rows>500000000000</read_rows>
          <execution_time>7200</execution_time>
      </interval>
  </statbox>

For the ``statbox`` quota, restrictions are set for every hour and for every 24 hours (86,400 seconds). The time interval is counted starting from an implementation-defined fixed moment in time. In other words, the 24-hour interval doesn't necessarily begin at midnight.

When the interval ends, all collected values are cleared. For the next hour, the quota calculation starts over.

Let's examine the amounts that can be restricted:

``queries`` - The overall number of queries.

``errors`` - The number of queries that threw exceptions.

``result_rows`` - The total number of rows output in results.

``read_rows`` - The total number of source rows retrieved from tables for running a query, on all remote servers.

``execution_time`` - The total time of query execution, in seconds (wall time).

If the limit is exceeded for at least one time interval, an exception is thrown with a text about which restriction was exceeded, for which interval, and when the new interval begins (when queries can be sent again).

Quotas can use the "quota key" feature in order to report on resources for multiple keys independently. Here is an example of this:

.. code-block:: xml

  <!-- For the global report builder. -->
  <web_global>
      <!-- keyed - the quota_key "key" is passed in the query parameter, and the quota is tracked separately for each key value.
      For example, you can pass a Metrica username as the key, so the quota will be counted separately for each username.
      Using keys makes sense only if quota_key is transmitted by the program, not by a user.
      You can also write <keyed_by_ip /> so the IP address is used as the quota key.
      (But keep in mind that users can change the IPv6 address fairly easily.) -->
      <keyed />

The quota is assigned to users in the ``users`` section of the config. See the section "Access rights".

For distributed query processing, the accumulated amounts are stored on the requestor server. So if the user goes to another server, the quota there will "start over".

When the server is restarted, quotas are reset.
