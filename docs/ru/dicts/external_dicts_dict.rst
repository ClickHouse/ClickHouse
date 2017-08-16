.. _dicts-external_dicts_dict:

**************************
Настройка внешнего словаря
**************************

Конфигурация словаря имеет следующую структуру:

.. code-block:: xml

  <dictionary>
      <name>dict_name</name>

      <source>
        <!-- Source configuration -->
      </source>

      <layout>
        <!-- Memory layout configuration -->
      </layout>

      <structure>
        <!-- Complex key configuration -->
      </structure>

      <lifetime>
        <!-- Lifetime of dictionary in memory -->
      </lifetime>
  </dictionary>

* name - Идентификатор, под которым словарь будет доступен для использования. Используйте символы ``[a-zA-Z0-9_\-]``.
* :ref:`source <dicts-external_dicts_dict_sources>` - Источник словаря.
* :ref:`layout <dicts-external_dicts_dict_layout>` - Размещение словаря в памяти.
* :ref:`structure <dicts-external_dicts_dict_structure>` - Структура словаря. Ключ и атрибуты, которые можно получить по ключу.
* :ref:`lifetime <dicts-external_dicts_dict_lifetime>` - Периодичность обновления словарей.
