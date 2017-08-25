.. _dicts-external_dicts_dict_structure:

*******************
Ключ и поля словаря
*******************

Секция ``<structure>`` описывает ключ словаря и поля, доступные для запросов.


Общий вид структуры:

.. code-block:: xml

      <dictionary>
          <structure>
              <id>
                  <name>Id</name>
              </id>
  
              <attribute>
                  <!-- Attribute parameters -->
              </attribute>
              
              ...

          </structure>
      </dictionary>

В структуре описываются столбцы:

* ``<id>`` - :ref:`ключевой столбец <dicts-external_dicts_dict_structure-key>`.
* ``<attribute>`` - :ref:`столбец данных <dicts-external_dicts_dict_structure-attributes>`. Столбцов может быть много.

.. _dicts-external_dicts_dict_structure-key:

Ключ
====

ClickHouse поддерживает следующие виды ключей:

* Числовой ключ. Формат UInt64. Описывается в теге ``<id>``.
* Составной ключ. Набор значений разного типа. Описывается в теге ``<key>``.
  
Структура может содержать либо ``<id>`` либо ``<key>``.


.. attention:: Ключ не надо дополнительно описывать в атрибутах.

Числовой ключ
--------------

Формат: ``UInt64``.

Пример конфигурации:

.. code-block:: xml

    <id>
        <name>Id</name>
    </id>


Поля конфигурации:

* name - имя столбца с ключами.
  

Составной ключ
---------------

Ключем может быть кортеж (``tuple``) из полей произвольных типов. :ref:`layout <dicts-external_dicts_dict_layout>` в этом случае должен быть ``complex_key_hashed`` или ``complex_key_cache``.

.. tip:: Cоставной ключ может состоять и из одного элемента, что даёт возможность использовать в качестве ключа, например, строку.

Структура ключа задаётся в элементе ``<key>``. Поля ключа задаются в том же формате, что и :ref:`атрибуты <dicts-external_dicts_dict_structure-attributes>` словаря. Пример:

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


При запросе в функции ``dictGet*`` в качестве ключа передаётся кортеж. Пример: ``dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))``.


.. _dicts-external_dicts_dict_structure-attributes:

Атрибуты
========

Пример конфигурации:

.. code-block:: xml

    <structure>
        ...
        <attribute>
            <name>Name</name>
            <type>Type</type>
            <null_value></null_value>
            <expression>rand64()</expression>
            <hierarchical>true</hierarchical>
            <injective>true</injective>
        </attribute>
    </structure>

Поля конфигурации:

* ``name`` - Имя столбца.
* ``type`` - Тип столбца. Задает способ интерпретации данных в источнике. Например, в случае MySQL, в таблице-источнике поле может быть ``TEXT``, ``VARCHAR``, ``BLOB``, но загружено может быть как ``String``.
* ``null_value`` - Значение по умолчанию для несуществующего элемента. В примере - пустая строка.
* ``expression`` - Атрибут может быть выражением. Тег не обязательный.
* ``hierarchical`` - Поддержка иерархии. Отображение в идентификатор родителя. По умолчанию, ``false``.
* ``injective`` - Признак инъективности отображения ``id -> attribute``. Если ``true``, то можно оптимизировать ``GROUP BY``. По умолчанию, ``false``.
