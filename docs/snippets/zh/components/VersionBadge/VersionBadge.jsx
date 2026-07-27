const VersionBadge = ({ minVersion }) => (
  <div className="versionBadge">
    <div className="versionIcon" style={{ marginRight: "8px", marginTop: "4px" }}>
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M5 14C5.82843 14 6.5 13.3284 6.5 12.5C6.5 11.6716 5.82843 11 5 11C4.17157 11 3.5 11.6716 3.5 12.5C3.5 13.3284 4.17157 14 5 14Z" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.25" />
        <path d="M5 5C5.82843 5 6.5 4.32843 6.5 3.5C6.5 2.67157 5.82843 2 5 2C4.17157 2 3.5 2.67157 3.5 3.5C3.5 4.32843 4.17157 5 5 5Z" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.25" />
        <path d="M13 10.5C13.8284 10.5 14.5 9.82843 14.5 9C14.5 8.17157 13.8284 7.5 13 7.5C12.1716 7.5 11.5 8.17157 11.5 9C11.5 9.82843 12.1716 10.5 13 10.5Z" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.25" />
        <path d="M11.5 9H9.5C9.03426 9 8.57493 8.89157 8.15836 8.68328C7.74179 8.475 7.37944 8.17259 7.1 7.8L5 5V11" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.25" />
      </svg>
    </div>
    自版本 {minVersion} 起可用
  </div>
);
export default VersionBadge;