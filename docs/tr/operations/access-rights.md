---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "Eri\u015Fim Kontrol\xFC ve hesap y\xF6netimi"
---

# Erişim Kontrolü ve hesap yönetimi {#access-control}

ClickHouse dayalı erişim kontrol yönetimini destekler [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) yaklaşmak.

ClickHouse erişim varlıkları:
- [Kullanıcı hesabı](#user-account-management)
- [Rol](#role-management)
- [Satır Politikası](#row-policy-management)
- [Ayarlar Profili](#settings-profiles-management)
- [Kota](#quotas-management)

Kullanarak erişim varlıkları yapılandırabilirsiniz:

-   SQL odaklı iş akışı.

    İhtiyacın var [etkinleştirmek](#enabling-access-control) bu işlevsellik.

-   Hizmetçi [yapılandırma dosyaları](configuration-files.md) `users.xml` ve `config.xml`.

SQL tabanlı iş akışı kullanmanızı öneririz. Her iki yapılandırma yöntemi de aynı anda çalışır; bu nedenle, hesapları ve erişim haklarını yönetmek için sunucu yapılandırma dosyalarını kullanırsanız, SQL tabanlı iş akışına yumuşak bir şekilde geçebilirsiniz.

!!! note "Uyarıcı"
    Aynı erişim varlığını her iki yapılandırma yöntemiyle aynı anda yönetemezsiniz.

## Kullanma {#access-control-usage}

Varsayılan olarak, ClickHouse sunucusu kullanıcı hesabını sağlar `default` SQL güdümlü erişim denetimi ve hesap yönetimi kullanılarak izin verilmez, ancak tüm haklara ve izinlere sahiptir. Bu `default` kullanıcı hesabı, kullanıcı adı tanımlanmadığında, örneğin istemciden girişte veya dağıtılmış sorgularda kullanılır. Dağıtılmış sorgu işleme varsayılan kullanıcı hesabı kullanılır, sunucu veya küme yapılandırması belirtmezse [kullanıcı ve şifre](../engines/table-engines/special/distributed.md) özellikler.

Sadece ClickHouse kullanmaya başlarsanız, aşağıdaki senaryoyu kullanabilirsiniz:

1.  [Etkinleştirmek](#enabling-access-control) SQL-driven erişim kontrolü ve hesap yönetimi için `default` kullanan.
2.  Log underin un underder the `default` kullanıcı hesabı ve gerekli tüm kullanıcıları oluşturun. Yönetici hesabı oluşturmayı unutmayın (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3.  [İzinleri kısıtla](settings/permissions-for-queries.md#permissions_for_queries) için `default` kullanıcı ve devre dışı SQL odaklı erişim kontrolü ve bunun için hesap yönetimi.

### Mevcut çözümün özellikleri {#access-control-properties}

-   Olmasa bile veritabanları ve tablolar için izinler verebilirsiniz.
-   Bir tablo silindiyse, bu tabloya karşılık gelen tüm ayrıcalıklar iptal edilmez. Bu nedenle, daha sonra aynı ada sahip yeni bir tablo oluşturulursa, tüm ayrıcalıklar tekrar gerçek olur. Silinen tabloya karşılık gelen ayrıcalıkları iptal etmek için, örneğin, `REVOKE ALL PRIVILEGES ON db.table FROM ALL` sorgu.
-   Ayrıcalıklar için ömür boyu ayarları yoktur.

## Kullanıcı hesabı {#user-account-management}

Bir kullanıcı hesabı, Clickhouse'da birisini yetkilendirmeye izin veren bir erişim varlığıdır. Bir kullanıcı hesabı içerir:

-   Kimlik bilgileri.
-   [Ayrıcalıklar](../sql-reference/statements/grant.md#grant-privileges) kullanıcı gerçekleştirebilir sorgular kapsamı tanımlayın.
-   ClickHouse sunucusuna bağlantının izin verildiği ana bilgisayarlar.
-   Verilen ve varsayılan roller.
-   Kullanıcının girişinde varsayılan olarak geçerli olan kısıtlamaları olan ayarlar.
-   Atanan ayarlar profilleri.

Bir kullanıcı hesabına ayrıcalıklar tarafından verilebilir [GRANT](../sql-reference/statements/grant.md) sorgu veya atama ile [roller](#role-management). Bir kullanıcının ayrıcalıklarını iptal etmek için, ClickHouse [REVOKE](../sql-reference/statements/revoke.md) sorgu. Bir kullanıcının ayrıcalıklarını listelemek için - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) deyim.

Yönetim sorguları:

-   [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
-   [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### Uygulama Ayarları {#access-control-settings-applying}

Ayarlar farklı şekillerde ayarlanabilir: bir kullanıcı hesabı için, verilen roller ve ayarlar profillerinde. Bir kullanıcı girişinde, farklı erişim varlıklarında bir ayar ayarlanırsa, bu ayarın değeri ve kısıtlamaları aşağıdaki öncelikler tarafından uygulanır (yüksekten düşüğe):

1.  Kullanıcı hesabı ayarı.
2.  Kullanıcı hesabının varsayılan rollerinin ayarları. Bazı rollerde bir ayar ayarlanmışsa, uygulama ayarının sırası tanımsızdır.
3.  Bir kullanıcıya veya varsayılan rollerine atanan ayarlar profilleri'ndeki ayarlar. Bazı profillerde bir ayar ayarlanmışsa, uygulama ayarı sırası tanımsızdır.
4.  Varsayılan olarak tüm sunucuya uygulanan ayarlar veya [varsayılan profil](server-configuration-parameters/settings.md#default-profile).

## Rol {#role-management}

Rol, bir kullanıcı hesabına verilebilecek erişim varlıkları için bir kapsayıcıdır.

Rol içerir:

-   [Ayrıcalıklar](../sql-reference/statements/grant.md#grant-privileges)
-   Ayarlar ve kısıtlamalar
-   Verilen roller listesi

Yönetim sorguları:

-   [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
-   [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
-   [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
-   [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

Bir rol için ayrıcalıklar tarafından verilebilir [GRANT](../sql-reference/statements/grant.md) sorgu. Bir rolden ayrıcalıkları iptal etmek için ClickHouse şunları sağlar: [REVOKE](../sql-reference/statements/revoke.md) sorgu.

## Satır Politikası {#row-policy-management}

Satır ilkesi, bir kullanıcı veya rol için hangi veya satırların kullanılabilir olduğunu tanımlayan bir filtredir. Satır ilkesi, belirli bir tablo ve bu satır ilkesini kullanması gereken rollerin ve/veya kullanıcıların listesi için filtreler içerir.

Yönetim sorguları:

-   [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
-   [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)

## Ayarlar Profili {#settings-profiles-management}

Ayarlar profili bir koleksiyon [ayarlar](settings/index.md). Ayarlar profili, ayarları ve kısıtlamaları ve bu kotanın uygulandığı rollerin ve/veya kullanıcıların listesini içerir.

Yönetim sorguları:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)

## Kota {#quotas-management}

Kota kaynak kullanımını sınırlar. Görmek [Kotalar](quotas.md).

Kota, bazı süreler için bir dizi sınır ve bu kotayı kullanması gereken rollerin ve/veya kullanıcıların listesini içerir.

Yönetim sorguları:

-   [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
-   [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)

## SQL tabanlı erişim denetimi ve hesap yönetimini etkinleştirme {#enabling-access-control}

-   Yapılandırmaları depolama için bir dizin Kur.

    ClickHouse, erişim varlık yapılandırmalarını, [access_control_path](server-configuration-parameters/settings.md#access_control_path) sunucu yapılandırma parametresi.

-   En az bir kullanıcı hesabı için SQL tabanlı erişim denetimi ve hesap yönetimini etkinleştirin.

    Varsayılan olarak SQL güdümlü erişim denetimi ve hesap yönetimi, tüm kullanıcılar için açık. En az bir kullanıcı yapılandırmanız gerekir `users.xml` yapılandırma dosyası ve atama 1 [access_management](settings/settings-users.md#access_management-user-setting) ayar.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
