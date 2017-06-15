XML
---

XML format is supported only for displaying data, not for INSERTS. Example:

.. code-block:: xml

  <?xml version='1.0' encoding='UTF-8' ?>
  <result>
          <meta>
                  <columns>
                          <column>
                                  <name>SearchPhrase</name>
                                  <type>String</type>
                          </column>
                          <column>
                                  <name>count()</name>
                                  <type>UInt64</type>
                          </column>
                  </columns>
          </meta>
          <data>
                  <row>
                          <SearchPhrase></SearchPhrase>
                          <field>8267016</field>
                  </row>
                  <row>
                          <SearchPhrase>bathroom interior</SearchPhrase>
                          <field>2166</field>
                  </row>
                  <row>
                          <SearchPhrase>yandex>
                          <field>1655</field>
                  </row>
                  <row>
                          <SearchPhrase>spring 2014 fashion</SearchPhrase>
                          <field>1549</field>
                  </row>
                  <row>
                          <SearchPhrase>free-form photo</SearchPhrase>
                          <field>1480</field>
                  </row>
                  <row>
                          <SearchPhrase>Angelina Jolie</SearchPhrase>
                          <field>1245</field>
                  </row>
                  <row>
                          <SearchPhrase>omsk</SearchPhrase>
                          <field>1112</field>
                  </row>
                  <row>
                          <SearchPhrase>photos of dog breeds</SearchPhrase>
                          <field>1091</field>
                  </row>
                  <row>
                          <SearchPhrase>curtain design</SearchPhrase>
                          <field>1064</field>
                  </row>
                  <row>
                          <SearchPhrase>baku</SearchPhrase>
                          <field>1000</field>
                  </row>
          </data>
          <rows>10</rows>
          <rows_before_limit_at_least>141137</rows_before_limit_at_least>
  </result>

If name of a column contains some unacceptable character, field is used as a name. In other aspects XML uses JSON structure.
As in case of JSON, invalid UTF-8 sequences are replaced by replacement character � so displayed text will only contain valid UTF-8 sequences.

In string values ``<`` and ``&`` are displayed as ``&lt;`` and ``&amp;``.

Arrays are displayed as ``<array><elem>Hello</elem><elem>World</elem>...</array>``,
and tuples as ``<tuple><elem>Hello</elem><elem>World</elem>...</tuple>``.
