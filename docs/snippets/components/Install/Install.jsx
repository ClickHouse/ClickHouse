// Mintlify snippets must use only local imports, so we render the selector
// with plain HTML/CSS instead of pulling `CardPrimary` from `@clickhouse/click-ui`.

const Card = ({title, icon, iconUrl, isSelected, onClick}) => (
  <button
    type="button"
    onClick={onClick}
    aria-pressed={isSelected}
    className="install-card"
    style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      gap: '0.5rem',
      padding: '0.75rem 0.5rem',
      minHeight: '90px',
      borderRadius: '8px',
      border: isSelected
        ? '1px solid #FAFF69'
        : '1px solid rgba(156, 163, 175, 0.4)',
      background: isSelected ? 'rgba(250, 255, 105, 0.08)' : 'transparent',
      cursor: 'pointer',
      transition: 'border-color 0.15s, background 0.15s',
      font: 'inherit',
      color: 'inherit',
    }}
  >
    {iconUrl ? (
      <img src={iconUrl} alt="" style={{height: '32px', width: '32px', objectFit: 'contain'}} />
    ) : icon ? (
      <span style={{fontSize: '1.6rem', lineHeight: 1}} aria-hidden="true">▸</span>
    ) : null}
    <span style={{fontSize: '0.85rem', fontWeight: 500, textAlign: 'center'}}>{title}</span>
  </button>
);

export const InstallSelector = (props) => {
  const [platform, setPlatform] = useState(null);

  const assetBase = typeof window !== 'undefined' && window.location.pathname.startsWith('/docs') ? '/docs' : '';
  const cards = [
    {key: 'CLI',        title: 'ClickHouse CLI', icon: 'terminal'},
    {key: 'Debian',     title: 'Debian/Ubuntu',  iconUrl: `${assetBase}/images/install/DebianUbuntu.svg`},
    {key: 'Redhat',     title: 'Redhat',         iconUrl: `${assetBase}/images/install/redhat.svg`},
    {key: 'LinuxOther', title: 'Other',          iconUrl: `${assetBase}/images/install/linux.svg`},
    {key: 'NixOS',      title: 'NixOS',          iconUrl: `${assetBase}/images/install/nixos.svg`},
    {key: 'MacOS',      title: 'MacOS',          iconUrl: `${assetBase}/images/install/apple.webp`},
    {key: 'Windows',    title: 'Windows',        iconUrl: `${assetBase}/images/install/windows.svg`},
    {key: 'Docker',     title: 'Docker',         iconUrl: `${assetBase}/images/install/docker.svg`},
  ];

  const propByKey = {
    CLI: props.cli,
    Debian: props.debian_prod,
    Ubuntu: props.debian_prod,
    Redhat: props.rpm_prod,
    LinuxOther: props.tar_prod,
    NixOS: props.nixos,
    MacOS: props.macos_prod,
    Windows: props.windows,
    Docker: props.docker,
  };

  return (
    <>
      <div
        className="prod-or-quick-install"
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
          gap: '0.75rem',
          margin: '1rem 0',
        }}
      >
        {cards.map(c => (
          <Card
            key={c.key}
            icon={c.icon}
            iconUrl={c.iconUrl}
            title={c.title}
            isSelected={platform === c.key}
            onClick={() => setPlatform(c.key)}
          />
        ))}
      </div>
      <div className="installInstructions">
        {propByKey[platform] || null}
      </div>
    </>
  );
};

export default InstallSelector;
