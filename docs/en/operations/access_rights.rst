Access rights
=============
Users and access rights are set up in the user config. This is usually ``users.xml``.

Users are recorded in the ``users`` section. Let's look at part of the ``users.xml`` file:

.. code-block:: xml

  <!-- Users and ACL. -->
  <users>
      <!-- If the username is not specified, the default user is used. -->
      <default>
          <!-- Password (in plaintext). May be empty. -->
          <password></password>
  
          <!-- List of networks that access is allowed from. Each list item has one of the following forms:
              <ip> IP address or subnet mask. For example, 222.111.222.3 or 10.0.0.1/8 or 2a02:6b8::3 or 2a02:6b8::3/64.
              <host> Host name. Example: example01. A DNS query is made for verification, and all received address are compared to the client address.
              <host_regexp> Regex for host names. For example, ^example\d\d-\d\d-\d\.yandex\.ru$
                  A DNS PTR query is made to verify the client address and the regex is applied to the result.
                  Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                  We strongly recommend that the regex ends with \.yandex\.ru$. If you are installing ClickHouse independently, here you should specify:
              <networks>
                  <ip>::/0</ip>
              </networks> -->
  
          <networks incl="networks" />
          <!-- Settings profile for the user. -->
          <profile>default</profile>
          <!-- Quota for the user. -->
          <quota>default</quota>
      </default>
  
      <!-- For queries from the user interface. -->
      <web>
          <password></password>
          <networks incl="networks" />
          <profile>web</profile>
          <quota>default</quota>
      </web>

Here we can see that two users are declared: ``default`` and ``web``. We added the ``web`` user ourselves.
The ``default`` user is chosen in cases when the username is not passed. The ``default`` user can also be used for distributed query processing - the system accesses remote servers using this username if no ``user`` and ``password`` were configured for that server inside cluster configuration (see also section about "Distributed" table engine).

For connection to the server inside cluster you should use the user without any substantial restrictions or quotas - otherwise, distributed queries will fail.

The password is specified in plain text directly in the config. In this regard, you should not consider these passwords as providing security against potential malicious attacks. Rather, they are necessary for protection from Yandex employees.

A list of networks is specified that access is allowed from. In this example, the list of networks for both users is loaded from a separate file (``/etc/metrika.xml``) containing the ``networks`` substitution. Here is a fragment of it:

.. code-block:: xml

  <yandex>
      ...
      <networks>
          <ip>::/64</ip>
          <ip>93.111.222.128/26</ip>
          <ip>2a02:6b8:0:111::/64</ip>
          ...
      </networks>
  </yandex>

We could have defined this list of networks directly in ``users.xml``, or in a file in the ``users.d`` directory (for more information, see the section "Configuration files").

The config includes comments explaining how to open access from everywhere.

For use in production, only specify IP elements (IP addresses and their masks), since using ``host`` and ``host_regexp`` might cause extra latency.

Next the user settings profile is specified (see the section "Settings profiles"). You can specify the default profile, ``default``. The profile can have any name. You can specify the same profile for different users. The most important thing you can write in the settings profile is ``readonly`` set to ``1``, which provides read-only access.

After this, the quota is defined (see the section "Quotas"). You can specify the default quota, ``default``. It is set in the config by default so that it only counts resource usage, but does not restrict it. The quota can have any name. You can specify the same quota for different users - in this case, resource usage is calculated for each user individually.
