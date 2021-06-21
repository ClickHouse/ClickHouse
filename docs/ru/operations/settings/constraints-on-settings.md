---
toc_priority: 62
toc_title: "\u041e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u044f\u0020\u043d\u0430\u0020\u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435\u0020\u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043a"
---

# Ограничения на изменение настроек {#constraints-on-settings}

Ограничения на изменение настроек могут находиться внутри секции `profiles` файла `user.xml` и запрещают пользователю менять некоторые настройки с помощью запроса `SET`.
Выглядит это следующим образом:

``` xml
<profiles>
  <имя_пользователя>
    <constraints>
      <настройка_1>
        <min>нижняя_граница</min>
      </настройка_1>
      <настройка_2>
        <max>верхняя_граница</max>
      </настройка_2>
      <настройка_3>
        <min>нижняя_граница</min>
        <max>верхняя_граница</max>
      </настройка_3>
      <настройка_4>
        <readonly/>
      </настройка_4>
    </constraints>
  </имя_пользователя>
</profiles>
```

Если пользователь пытается выйти за пределы, установленные этими ограничениями, то кидается исключение и настройка сохраняет прежнее значение.
Поддерживаются три типа ограничений: `min`, `max` и `readonly`. Ограничения `min` и `max` указывают нижнюю и верхнюю границы для числовых настроек и могут использоваться вместе.
Ограничение `readonly` указывает, что пользователь не может менять настройку.

**Пример:** Пусть файл `users.xml` содержит строки:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

Каждый из следующих запросов кинет исключение:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**Примечание:** профиль с именем `default` обрабатывается специальным образом: все ограничения на изменение настроек из этого профиля становятся дефолтными и влияют на всех пользователей, кроме тех, где эти ограничения явно переопределены.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/settings/constraints_on_settings/) <!--hide-->
