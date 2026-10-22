

export const ClickPipesAnimationV2 = ({ className, ...props }) => {
  const LogoItem = ({ src, alt, active, badge }) => {
    return (
        <div className={`relative rounded-md bg-neutral-900 dark:bg-neutral-900 p-4 w-full h-full flex items-center justify-center box-border transition-colors duration-200 hover:bg-neutral-800 dark:hover:bg-neutral-800 ${
          active ? 'border border-[#FAFF69]' : 'border border-neutral-700/80 grayscale'
        }`}>
          {badge && (
              <div className='absolute -right-2 -top-2 rounded-full bg-[#FAFF69] px-3 text-xs font-normal text-neutral-900 z-10'>
                {badge}
              </div>
          )}
          <img
              src={src}
              width={40}
              height={40}
              alt={alt}
              loading="lazy"
              className="h-10 w-10 max-h-10 max-w-10 object-contain block"
          />
        </div>
    )
  };

  return (
      <div className={`${className || ''}`} {...props} style={{ display: 'inline-block', position: 'relative' }}>
        <div className="relative flex flex-row items-center justify-start" style={{ maxWidth: '100%', width: 'fit-content' }}>
          {/* Logos */}
          <ul className="grid grid-cols-3 grid-rows-4 gap-4 list-none p-0 m-0" style={{ gridTemplateColumns: 'repeat(3, minmax(64px, 72px))', gridTemplateRows: 'repeat(4, minmax(64px, 72px))' }}>
            {/* Row 1 */}
            <li className="block m-0 p-0">
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/kafka.svg'
                  alt='Kafka'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/amazon_s3.svg'
                  alt='AWS S3'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/mysql.svg'
                  alt='MySQL CDC'
                  active={true}
              />
            </li>
            {/* Row 2 */}
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/diagram/confluent-logos-idXfleyO4U-1.svg'
                  alt='Confluent'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/diagram/azure-event-hub.svg'
                  alt='Azure Event Hubs'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/postgres.svg'
                  alt='Postgres CDC'
                  active={true}
              />
            </li>
            {/* Row 3 */}
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/diagram/aws-msk.svg'
                  alt='AWS MSK'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/redpanda.svg'
                  alt='RedPanda'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/google-cloud-storage.svg'
                  alt='Google Cloud Storage'
                  active={true}
              />
            </li>
            {/* Row 4 */}
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/diagram/aws-kinesis.svg'
                  alt='AWS Kinesis'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/azure-blob-storage.svg'
                  alt='Azure Blob Storage'
                  active={true}
              />
            </li>
            <li>
              <LogoItem
                  src='https://clickhouse.com/images/cloud/integrations/digitalocean.svg'
                  alt='DigitalOcean'
                  active={true}
              />
            </li>
          </ul>

          {/* Lines */}
          <div className='shrink-0 grow-0 -ml-44 block'>
            <svg
                xmlns='http://www.w3.org/2000/svg'
                width='316'
                height='269'
                viewBox='0 0 316 269'
                className='block w-[316px] h-[269px]'>
              <g fill='none' fillRule='evenodd' strokeWidth='3'>
                <g stroke='#524D4D'>
                  <path d='M1 267h229.5a6 6 0 0 0 6-6V137.5a6 6 0 0 1 6-6H315' />
                  <path d='M1 178h229.5a6 6 0 0 0 6-6v-35a6 6 0 0 1 6-6H315' />
                  <path d='M1 89h229.5a6 6 0 0 1 6 6v30a6 6 0 0 0 6 6H315' />
                  <path d='M1 1h229.5a6 6 0 0 1 6 6v118a6 6 0 0 0 6 6H315' />
                </g>
                <g stroke='#FAFF69'>
                  {/* Row 1 line */}
                  <path d='M1 267h229.5a6 6 0 0 0 6-6V137.5a6 6 0 0 1 6-6H315'>
                    <animate
                        dur='3.5s'
                        attributeName='stroke-dasharray'
                        repeatCount='indefinite'
                        values='0,0,0,0,0,621; 0,0,0,621,311,0; 0,0,221,311,0,0; 0,621,0,621,0,0;'
                    />
                  </path>

                  {/* Row 2 line */}
                  <path d='M1 178h229.5a6 6 0 0 0 6-6v-35a6 6 0 0 1 6-6H315'>
                    <animate
                        dur='2.5s'
                        attributeName='stroke-dasharray'
                        repeatCount='indefinite'
                        begin='0.5s;op.end+2.5s'
                        values='0,0,0,0,0,621; 0,0,0,311.25,311.25,0; 0,0,155.625,466.875,0,0; 0,311.25,0,311.25,0,0'
                    />
                  </path>

                  {/* Row 3 line */}
                  <path d='M1 89h229.5a6 6 0 0 1 6 6v30a6 6 0 0 0 6 6H315'>
                    <animate
                        dur='3s'
                        attributeName='stroke-dasharray'
                        repeatCount='indefinite'
                        begin='0.5s;op.end+2.5s'
                        values='0,0,0,0,0,621; 0,0,0,311.25,311.25,0; 0,0,155.625,466.875,0,0; 0,311.25,0,311.25,0,0'
                    />
                  </path>

                  {/* Row 4 line */}
                  <path d='M1 1h229.5a6 6 0 0 1 6 6v118a6 6 0 0 0 6 6H315'>
                    <animate
                        dur='4s'
                        attributeName='stroke-dasharray'
                        repeatCount='indefinite'
                        values='0,0,0,0,0,621; 0,0,0,621,311,0; 0,0,221,311,0,0; 0,621,0,621,0,0;'
                    />
                  </path>
                </g>
              </g>
            </svg>
          </div>

          {/* ClickHouse */}
          <div className='shrink-0 grow-0 block'>
            <div className='flex h-[120px] w-[120px] items-center justify-center rounded-md bg-[#FAFF69]' style={{ boxShadow: '0px 8px 30px rgba(252, 255, 116, 0.5)' }}>
              <svg
                  xmlns='http://www.w3.org/2000/svg'
                  width='68'
                  height='68'
                  viewBox='0 0 68 68'
                  className='w-[68px] h-[68px]'>
                <path
                    fill='#282804'
                    d='M6.61857.079C7.424591.079 8.078.7324091 8.078 1.53843v64.57884c0 .806021-.653409 1.45943-1.45943 1.45943H2.13143c-.806021 0-1.45943-.653409-1.45943-1.45943V1.53843C.672.7324091 1.325409.079 2.13143.079h4.48714Zm14.812 0c.806021 0 1.45943.6534091 1.45943 1.45943v64.57884c0 .806021-.653409 1.45943-1.45943 1.45943h-4.48714c-.806021 0-1.45943-.653409-1.45943-1.45943V1.53843c0-.8060209.653409-1.45943 1.45943-1.45943h4.48714Zm14.814 0c.806021 0 1.45943.6534091 1.45943 1.45943v64.57884c0 .806021-.653409 1.45943-1.45943 1.45943h-4.48714c-.806021 0-1.45943-.653409-1.45943-1.45943V1.53843c0-.8060209.653409-1.45943 1.45943-1.45943h4.48714Zm14.808 0c.806021 0 1.45943.6534091 1.45943 1.45943v64.57884c0 .806021-.653409 1.45943-1.45943 1.45943h-4.48714c-.806021 0-1.45943-.653409-1.45943-1.45943V1.53843c0-.8060209.653409-1.45943 1.45943-1.45943h4.48714Zm14.817 26.251c.806021 0 1.45943.653409 1.45943 1.45943v12.08064c0 .806021-.653409 1.45943-1.45943 1.45943h-4.48714c-.806021 0-1.45943-.653409-1.45943-1.45943V27.78943c0-.806021.653409-1.45943 1.45943-1.45943h4.48714Z'
                />
              </svg>
            </div>
          </div>
        </div>
      </div>
  )
};