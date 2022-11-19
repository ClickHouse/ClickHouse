# Kerberos认证 {#external-authenticators-kerberos} 
现有正确配置的 ClickHouse 用户可以通过 Kerberos 身份验证协议进行身份验证.

目前, Kerberos 只能用作现有用户的外部身份验证器，这些用户在 `users.xml` 或本地访问控制路径中定义.
这些用户只能使用 HTTP 请求, 并且必须能够使用 GSS-SPNEGO 机制进行身份验证.

对于这种方法, 必须在系统中配置 Kerberos, 且必须在 ClickHouse 配置中启用.

## 开启Kerberos {#enabling-kerberos-in-clickHouse}
要启用 Kerberos, 应该在 `config.xml` 中包含 `kerberos` 部分. 此部分可能包含其他参数.

#### 参数: {#parameters}
- `principal` - 将在接受安全上下文时获取和使用的规范服务主体名称.
- 此参数是可选的, 如果省略, 将使用默认主体.

- `realm` - 一个领域, 用于将身份验证限制为仅那些发起者领域与其匹配的请求.

- 此参数是可选的，如果省略，则不会应用其他领域的过滤.

示例 (进入 `config.xml`):
```xml
<yandex>
    <!- ... -->
    <kerberos />
</yandex>
```

主体规范:
```xml
<yandex>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</yandex>
```

按领域过滤:
```xml
<yandex>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</yandex>
```

!!! warning "注意"

您只能定义一个 `kerberos` 部分. 多个 `kerberos` 部分的存在将强制 ClickHouse 禁用 Kerberos 身份验证.

!!! warning "注意"

`主体`和`领域`部分不能同时指定. `主体`和`领域`的出现将迫使ClickHouse禁用Kerberos身份验证.

## Kerberos作为现有用户的外部身份验证器 {#kerberos-as-an-external-authenticator-for-existing-users}
Kerberos可以用作验证本地定义用户(在`users.xml`或本地访问控制路径中定义的用户)身份的方法。目前，**只有**通过HTTP接口的请求才能被认证(通过GSS-SPNEGO机制).

Kerberos主体名称格式通常遵循以下模式:
- *primary/instance@REALM*

*/instance* 部分可能出现零次或多次. **发起者的规范主体名称的主要部分应与被认证用户名匹配, 以便身份验证成功**.

### `users.xml`中启用Kerberos {#enabling-kerberos-in-users-xml}
为了启用用户的 Kerberos 身份验证, 请在用户定义中指定 `kerberos` 部分而不是`密码`或类似部分.

参数:
- `realm` - 用于将身份验证限制为仅那些发起者的领域与其匹配的请求的领域.
- 此参数是可选的, 如果省略, 则不会应用其他按领域的过滤.

示例 (进入 `users.xml`):
```
<yandex>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</yandex>
```

!!! warning "警告"

注意, Kerberos身份验证不能与任何其他身份验证机制一起使用. 任何其他部分(如`密码`和`kerberos`)的出现都会迫使ClickHouse关闭.

!!! info "提醒"

请注意, 现在, 一旦用户 `my_user` 使用 `kerberos`, 必须在主 `config.xml` 文件中启用 Kerberos，如前所述.

### 使用 SQL 启用 Kerberos {#enabling-kerberos-using-sql}
在 ClickHouse 中启用 [SQL 驱动的访问控制和帐户管理](https://clickhouse.com/docs/en/operations/access-rights/#access-control)后, 也可以使用 SQL 语句创建由 Kerberos 识别的用户.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...或者, 不按领域过滤:
```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```
