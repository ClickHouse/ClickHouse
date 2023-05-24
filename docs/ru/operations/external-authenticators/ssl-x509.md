# Аутентификация по сертификату SSL X.509  {#ssl-external-authentication}

[Опция 'strict'](../server-configuration-parameters/settings.md#server_configuration_parameters-openssl) включает обязательную проверку сертификатов входящих соединений в библиотеке `SSL`. В этом случае могут быть установлены только соединения, представившие действительный сертификат. Соединения с недоверенными сертификатами будут отвергнуты. Таким образом, проверка сертификата позволяет однозначно аутентифицировать входящее соединение. Идентификация пользователя осуществляется по полю `Common Name` сертификата. Это позволяет ассоциировать несколько сертификатов с одним и тем же пользователем. Дополнительно, перевыпуск и отзыв сертификата не требуют изменения конфигурации ClickHouse.

Для включения аутентификации по SSL сертификату, необходимо указать список `Common Name` для каждого пользователя ClickHouse в файле настройки `config.xml`:

**Example**
```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name>
            <certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- More names -->
            </certificates>
            <!-- Other settings -->
        </user_name>
    </users>
</clickhouse>
```

Для правильной работы SSL [`chain of trust`](https://en.wikipedia.org/wiki/Chain_of_trust) важно также убедиться в правильной настройке параметра [`caConfig`](../server-configuration-parameters/settings.md#server_configuration_parameters-openssl)
