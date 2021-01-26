---
title: Какой версией ClickHouse пользоваться?
toc_hidden: true
toc_priority: 10
---

# Какую версию ClickHouse использовать? {#which-clickhouse-version-to-use-in-production}

Во-первых, давайте обсудим, почему люди задают этот вопрос сразу. Две основные причины:

1.  ClickHouse разработали достаточно быстро и обычно есть более 10-ти стабильных релизов в год. Так что есть из чего выбирать, а это не всегда просто. 
2.  Некоторые пользователи хотят избежать трат времени на выяснение, какая версия работает лучше для их сценария использования и просто хотят послушать толковый совет.

Вторая причина более весомая, так что начнем с нее и затем вернемся к навигации по существующим релизам ClickHouse.

## Какую версию ClickHouse вы посоветуете? {#which-clickhouse-version-do-you-recommend}

Очень удобный вариант — нанять консультанта или довериться известному эксперту, чтобы делегировать ответственность за вашу производственную среду. Вы устанавливаете одну из верси ClickHouse, которую порекомендовал кто-то, а теперь если с ней что-то идет не так — это уже не ваша вина, а тех, кто давал совет. Такая линия причинно-следственной связи очень большая ловушка. Никто другой, кроме вас не знает, что происходит в производственной среде вашей компании. 

Так что как верно выбрать версию ClickHouse, до которой стоит обновиться? Или как выбрать версию, если вы только начинаете пользоваться ClickHouse? Во-первых, вам стоит вложить деньги в настройку **реалистичной предпроизводственной среды**. В идеальном мире, это может быть полностью идентичная теневая копия, чаще всего дорогостоящая.

Вот некоторые ключевые моменты для получения разумной точности в предпроизводственной среде с невысокой стоимостью:

-   Предпроизводственная среда должна обрабатывать максимально близкий набор запросов к тому, который вы планируете обрабатывать в реальной работе:
    -   Не делайте ее в режиме "только чтения" с некоторыми замороженными данными.
    -   Не делайте ее в режиме "только запись" только с копированием данных без создания некоторых типичных отчетов.
    -   Не стирайте все подчистую вместо того, чтобы применить схему миграции.
-   Пользуйтесь сэмплом реальных рабочих данных и запросов. Попробуйте выбрать репрезентативный сэмпл, который возвращает адекватные результаты по запросу `SELECT`. Также пользуйтесь обфускацией, когда ваши данные чувствительные, а по внутренним правилам не позволяется сменить среду разработки.
-   Убедитесь, что вы мониторите предпродакшн, а также у вас есть ПО, котороое оповестит вас о том, что происходит в таком же виде, как и ваше рабочее окружение.
-   Если ваш продакшн распределен по разным датацентрам или регионам, предпродакшн должен быть таким же.
-   If your production uses complex features like replication, distributed table, cascading materialize views, make sure they are configured similarly in pre-production.
-   There’s a trade-off on using the roughly same number of servers or VMs in pre-production as in production, but of smaller size, or much less of them, but of the same size. The first option might catch extra network-related issues, while the latter is easier to manage.

Второе направление инвестиций — **инфраструктура автоматизированного тестирования**. Не думайте, что если некоторый вид запроса был успешно выполнен однажды, то так будет продолжаться всегда. Это нормально иметь некоторые юнит-тесты, где It’s ok to have some unit tests where ClickHouse is mocked but make sure your product has a reasonable set of automated tests that are run against real ClickHouse and check that all important use cases are still working as expected.

Extra step forward could be contributing those automated tests to [ClickHouse’s open-source test infrastructure](https://github.com/ClickHouse/ClickHouse/tree/master/tests) that’s continuously used in its day-to-day development. It definitely will take some additional time and effort to learn [how to run it](../../development/tests.md) and then how to adapt your tests to this framework, but it’ll pay off by ensuring that ClickHouse releases are already tested against them when they are announced stable, instead of repeatedly losing time on reporting the issue after the fact and then waiting for a bugfix to be implemented, backported and released. Some companies even have such test contributions to infrastructure by its use as an internal policy, most notably it’s called [Beyonce’s Rule](https://www.oreilly.com/library/view/software-engineering-at/9781492082781/ch01.html#policies_that_scale_well) at Google.

When you have your pre-production environment and testing infrastructure in place, choosing the best version is straightforward:

1.  Routinely run your automated tests against new ClickHouse releases. You can do it even for ClickHouse releases that are marked as `testing`, but going forward to the next steps with them is not recommended.
2.  Deploy the ClickHouse release that passed the tests to pre-production and check that all processes are running as expected.
3.  Report any issues you discovered to [ClickHouse GitHub Issues](https://github.com/ClickHouse/ClickHouse/issues).
4.  If there were no major issues, it should be safe to start deploying ClickHouse release to your production environment. Investing in gradual release automation that implements an approach similar to [canary releases](https://martinfowler.com/bliki/CanaryRelease.html) or [green-blue deployments](https://martinfowler.com/bliki/BlueGreenDeployment.html) might further reduce the risk of issues in production.

As you might have noticed, there’s nothing specific to ClickHouse in the approach described above, people do that for any piece of infrastructure they rely on if they take their production environment seriously.

## Как выбрать между релизами ClickHouse? {#how-to-choose-between-clickhouse-releases}

If you look into contents of ClickHouse package repository, you’ll see four kinds of packages:

1.  `testing`
2.  `prestable`
3.  `stable`
4.  `lts` (long-term support)

As was mentioned earlier, `testing` is good mostly to notice issues early, running them in production is not recommended because each of them is not tested as thoroughly as other kinds of packages.

`prestable` is a release candidate which generally looks promising and is likely to become announced as `stable` soon. You can try them out in pre-production and report issues if you see any.

For production use, there are two key options: `stable` and `lts`. Here is some guidance on how to choose between them:

-   `stable` is the kind of package we recommend by default. They are released roughly monthly (and thus provide new features with reasonable delay) and three latest stable releases are supported in terms of diagnostics and backporting of bugfixes.
-   `lts` are released twice a year and are supported for a year after their initial release. You might prefer them over `stable` in the following cases:
    -   Your company has some internal policies that don’t allow for frequent upgrades or using non-LTS software.
    -   You are using ClickHouse in some secondary products that either doesn’t require any complex ClickHouse features and don’t have enough resources to keep it updated.

Many teams who initially thought that `lts` is the way to go, often switch to `stable` anyway because of some recent feature that’s important for their product.

!!! warning "Important"
    One more thing to keep in mind when upgrading ClickHouse: we’re always keeping eye on compatibility across releases, but sometimes it’s not reasonable to keep and some minor details might change. So make sure you check the [changelog](../../whats-new/changelog/index.md) before upgrading to see if there are any notes about backward-incompatible changes.
