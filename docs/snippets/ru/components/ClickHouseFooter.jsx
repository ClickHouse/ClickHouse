const ClickHouseFooter = () => {
  return (
    <div className="flex flex-col bg-neutral-900 pb-8 pt-16 -mx-[100vw] px-[100vw] mt-20">
      <div className="mx-auto w-full max-w-7xl justify-between gap-8 pb-11 md:flex lg:gap-10">
        <div className="flex w-full flex-col">
          <div className="sitemap flex flex-col gap-y-8 lg:flex-row lg:gap-x-3">
            {/* Столбец продуктов */}
            <div className="flex flex-col lg:w-4/12">
              <h3 className="mb-3 font-inter text-sm font-semibold text-neutral-100">Продукты</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/cloud">
                    ClickHouse Cloud
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/clickhouse">
                    ClickHouse
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/cloud/bring-your-own-cloud">
                    Собственное облако
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/government">
                    ClickHouse Government
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/clickhouse/keeper">
                    ClickHouse Keeper
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/use-cases/observability">
                    ClickStack
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/cloud/clickpipes">
                    ClickPipes
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/integrations">
                    Интеграции
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/chdb">
                    chDB
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://trust.clickhouse.com">
                    Центр доверия
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/pricing">
                    Цены
                  </a>
                </li>
              </ul>
            </div>

            {/* Столбец ресурсов */}
            <div className="flex flex-col lg:w-4/12">
              <h3 className="mb-3 font-inter text-sm font-semibold text-neutral-100">Ресурсы</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/docs">
                    Документация
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/learn">
                    Обучение
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/support/program">
                    Поддержка
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://benchmark.clickhouse.com">
                    Бенчмарки
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/use-cases">
                    Сценарии использования
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/videos">
                    Видео
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/demos">
                    Демо
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/real-time-data-warehouse">
                    Хранилище данных в реальном времени
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/videos?category=open-house">
                    Видео Open House
                  </a>
                </li>
              </ul>
            </div>

            {/* Столбец компании */}
            <div className="flex flex-col lg:w-4/12">
              <h3 className="mb-3 font-inter text-sm font-semibold text-neutral-100">Компания</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/blog">
                    Блог
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/company/our-story">
                    О нас
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/company/careers">
                    Вакансии
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/company/contact?loc=footer">
                    Связаться с нами
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/company/events">
                    Мероприятия
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/company/news">
                    Новости
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/media">
                    Медиа
                  </a>
                </li>
              </ul>
            </div>

            {/* Community Column */}
            <div className="flex flex-col lg:w-4/12">
              <h3 className="mb-3 font-inter text-sm font-semibold text-neutral-100">Присоединяйтесь к сообществу</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://github.com/ClickHouse/ClickHouse">
                    GitHub
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/slack">
                    Slack
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://www.linkedin.com/company/clickhouseinc">
                    LinkedIn
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://x.com/ClickhouseDB">
                    X
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://bsky.app/profile/clickhouse.com">
                    Bluesky
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://telegram.me/clickhouse_en">
                    Telegram
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://www.meetup.com/pro/clickhouse">
                    Meetup
                  </a>
                </li>
              </ul>
            </div>

            {/* Comparisons & Partners Column */}
            <div className="flex flex-col lg:w-4/12">
              <h3 className="mb-3 font-inter text-sm font-semibold text-neutral-100">Сравнения</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/comparison/bigquery">
                    BigQuery
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/comparison/postgresql">
                    PostgreSQL
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/comparison/redshift">
                    Redshift
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/comparison/snowflake">
                    Snowflake
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/comparison/elastic-for-observability">
                    Elastic
                  </a>
                </li>
              </ul>

              <h3 className="mb-3 mt-8 font-inter text-sm font-semibold text-neutral-100">Партнёры</h3>
              <ul className="flex flex-col gap-y-2">
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/partners/aws">
                    AWS
                  </a>
                </li>
                <li>
                  <a target="_blank" className="text-sm text-neutral-400 hover:text-neutral-100 transition-colors" href="https://clickhouse.com/partners/azure">
                    Azure
                  </a>
                </li>
              </ul>
            </div>
          </div>
        </div>

        {/* Newsletter/GitHub Section */}
        <div className="flex flex-col pt-12 md:w-fit md:pt-0 md:min-w-[300px]">
          <img alt="ClickHouse logo" width="135" height="40" className="mb-4" src="https://clickhouse.com/_next/static/media/logo-full.ac8102d5.svg" />
          <div className="mb-4 text-sm text-neutral-400">Следите за новыми возможностями, дорожной картой продукта, обновлениями поддержки и облачными предложениями!</div>

          <a className="flex justify-end w-full mt-6" target="_blank" rel="noopener noreferrer" href="https://github.com/ClickHouse/ClickHouse">
            <button className="w-full md:w-fit bg-white text-black px-6 py-3 rounded-lg font-semibold hover:bg-gray-100 transition-colors flex items-center justify-center gap-2">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path
                  fillRule="evenodd"
                  clipRule="evenodd"
                  d="M8 1.75C4.27062 1.75 1.25 4.77062 1.25 8.5C1.25 11.4869 3.18219 14.0097 5.86531 14.9041C6.20281 14.9631 6.32937 14.7606 6.32937 14.5834C6.32937 14.4231 6.32094 13.8916 6.32094 13.3263C4.625 13.6384 4.18625 12.9128 4.05125 12.5331C3.97531 12.3391 3.64625 11.74 3.35938 11.5797C3.12312 11.4531 2.78562 11.1409 3.35094 11.1325C3.8825 11.1241 4.26219 11.6219 4.38875 11.8244C4.99625 12.8453 5.96656 12.5584 6.35469 12.3813C6.41375 11.9425 6.59094 11.6472 6.785 11.4784C5.28312 11.3097 3.71375 10.7275 3.71375 8.14563C3.71375 7.41156 3.97531 6.80406 4.40563 6.33156C4.33812 6.16281 4.10187 5.47094 4.47312 4.54281C4.47312 4.54281 5.03844 4.36563 6.32937 5.23469C6.86937 5.08281 7.44313 5.00687 8.01688 5.00687C8.59063 5.00687 9.16438 5.08281 9.70438 5.23469C10.9953 4.35719 11.5606 4.54281 11.5606 4.54281C11.9319 5.47094 11.6956 6.16281 11.6281 6.33156C12.0584 6.80406 12.32 7.40312 12.32 8.14563C12.32 10.7359 10.7422 11.3097 9.24031 11.4784C9.485 11.6894 9.69594 12.0944 9.69594 12.7272C9.69594 13.63 9.6875 14.3556 9.6875 14.5834C9.6875 14.7606 9.81406 14.9716 10.1516 14.9041C12.8178 14.0097 14.75 11.4784 14.75 8.5C14.75 4.77062 11.7294 1.75 8 1.75Z"
                  fill="currentColor"
                />
              </svg>
              <span>Поставьте звезду на GitHub</span>
            </button>
          </a>
        </div>
      </div>

      {/* Divider */}
      <div className="w-full max-w-7xl mx-auto border-t border-neutral-700 opacity-50"></div>

      {/* Copyright and Links */}
      <div className="flex flex-col items-start pt-8">
        <div className="mx-auto w-full max-w-7xl flex flex-col items-center gap-3 pt-4 text-center text-sm text-neutral-400 sm:gap-1 md:flex-row md:justify-between md:pt-0 md:text-left">
          <div>© {new Date().getFullYear()} ClickHouse, Inc. Штаб-квартира в Бэй-Эрии, Калифорния, и Амстердаме, Нидерланды.</div>
          <div className="flex flex-wrap items-center justify-center gap-4">
            <a href="https://clickhouse.com/legal/trademark-policy" target="_blank" rel="noopener noreferrer" className="whitespace-nowrap hover:text-neutral-100 transition-colors">
              Товарный знак
            </a>
            <a href="https://clickhouse.com/legal/privacy-policy" target="_blank" rel="noopener noreferrer" className="whitespace-nowrap hover:text-neutral-100 transition-colors">
              Конфиденциальность
            </a>
            <a href="https://trust.clickhouse.com/" target="_blank" rel="noopener noreferrer" className="whitespace-nowrap hover:text-neutral-100 transition-colors">
              Безопасность
            </a>
            <a href="https://clickhouse.com/legal" target="_blank" rel="noopener noreferrer" className="whitespace-nowrap hover:text-neutral-100 transition-colors">
              Правовая информация
            </a>
            <a href="https://clickhouse.com/legal/cookie-policy" target="_blank" rel="noopener noreferrer" className="whitespace-nowrap hover:text-neutral-100 transition-colors">
              Политика cookie
            </a>
          </div>
        </div>
      </div>
    </div>
  )
}

export default ClickHouseFooter;