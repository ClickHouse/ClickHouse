export const ClickStackAnimation = ({ className, ...props }) => {
  return (
    <div className={`clickstack-animation-wrapper ${className || ''}`} style={{ height: '537px' }} {...props}>
      <div className="clickstack-animation-inner" style={{ aspectRatio: '537 / 537', transform: 'scale(1)' }}>
        <div className="clickstack-grid">
          {/* Top Left - ClickHouse logo with animation */}
          <div className="clickstack-grid-item">
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" fill="none" viewBox="0 0 40 40">
                <path fill="#f5a800" d="M21.05 21.77a4.35 4.35 0 1 0 6.03 6.27 4.35 4.35 0 0 0-6.03-6.27m4.6 4.6a2.16 2.16 0 0 1-3.67-1.52 2.16 2.16 0 0 1 4.3 0c0 .57-.23 1.12-.63 1.52M27.12.4l-2.65 2.67a1.34 1.34 0 0 0 0 1.9l10.4 10.4c.52.52 1.36.52 1.89 0l2.66-2.67c.52-.52.52-1.37 0-1.9L29.02.4a1.34 1.34 0 0 0-1.9 0M8.47 34.42a1.2 1.2 0 0 0 0-1.72l-1.35-1.36a1.2 1.2 0 0 0-1.72 0l-2.8 2.79-.75-.77a1.1 1.1 0 0 0-1.54 0 1.1 1.1 0 0 0 0 1.54l4.61 4.6a1.1 1.1 0 0 0 1.54 0 1.1 1.1 0 0 0 0-1.53l-.77-.76 2.8-2.8z" />
                <path fill="#425cc7" d="m22.56 8.03-5.92 5.92a1.37 1.37 0 0 0 0 1.91l3.65 3.65a6.6 6.6 0 0 1 8.53.7l2.95-2.95a1.37 1.37 0 0 0 0-1.92l-7.3-7.3a1.36 1.36 0 0 0-1.91 0m-3.8 13.03L16.6 18.9a1.3 1.3 0 0 0-1.84 0l-7.6 7.6a1.3 1.3 0 0 0 0 1.85l4.3 4.31a1.3 1.3 0 0 0 1.84 0l4.89-4.9a6.7 6.7 0 0 1 .57-6.7" />
              </svg>
            </div>
            <svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100" className="clickstack-svg-absolute clickstack-svg-left-center">
              <path fill="none" className="clickstack-line-bg" strokeWidth="3" d="M1 0V75A25 25 0 0 0 25 99H100" pathLength="1000" />
              <path fill="none" className="clickstack-line-animated" strokeLinecap="round" strokeWidth="3" d="M1 0V75A25 25 0 0 0 25 99H100" pathLength="1000">
                <animate attributeName="stroke-dasharray" dur="3s" keyTimes="0;0.5;1" repeatCount="indefinite" values="100 900;500 500;100 900" />
                <animate attributeName="stroke-dashoffset" calcMode="linear" dur="3s" keyTimes="0;0.5;1" repeatCount="indefinite" values="0;-1000;-2000" />
              </path>
            </svg>
          </div>

          {/* Top Center - Server with vertical line */}
          <div className="clickstack-grid-column">
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="44" height="44" fill="none" viewBox="0 0 44 44">
                <path stroke="#dfdfdf" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M36.67 3.67H7.33a3.67 3.67 0 0 0-3.66 3.66v7.34a3.67 3.67 0 0 0 3.66 3.66h29.34a3.67 3.67 0 0 0 3.66-3.66V7.33a3.67 3.67 0 0 0-3.66-3.66m0 22H7.33a3.67 3.67 0 0 0-3.66 3.66v7.34a3.67 3.67 0 0 0 3.66 3.66h29.34a3.67 3.67 0 0 0 3.66-3.66v-7.34a3.67 3.67 0 0 0-3.66-3.66" />
                <path stroke="#dfdfdf" strokeLinecap="round" strokeLinejoin="round" strokeWidth="3.5" d="M11 11h.02M11 33h.02" />
              </svg>
            </div>
            <svg xmlns="http://www.w3.org/2000/svg" width="3" height="115" viewBox="0 0 3 115">
              <line x1="0" x2="0" y1="0" y2="115" strokeWidth="3" className="clickstack-line-bg" />
              <line x1="0" x2="0" y1="0" y2="115" strokeWidth="3" className="clickstack-line-animated">
                <animate attributeName="y1" values="-115;345;-57.5;230;-126.5;241.5;0" keyTimes="0;0.304;0.304;0.652;0.652;1;1" dur="5.75s" repeatCount="indefinite" />
                <animate attributeName="y2" values="0;575;0;402.5;0;356.5;0" keyTimes="0;0.304;0.304;0.652;0.652;1;1" dur="5.75s" repeatCount="indefinite" />
              </line>
            </svg>
          </div>

          {/* Top Right - ClickHouse logo with animation */}
          <div className="clickstack-grid-item">
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" fill="none" viewBox="0 0 40 40">
                <path fill="#f5a800" d="M21.05 21.77a4.35 4.35 0 1 0 6.03 6.27 4.35 4.35 0 0 0-6.03-6.27m4.6 4.6a2.16 2.16 0 0 1-3.67-1.52 2.16 2.16 0 0 1 4.3 0c0 .57-.23 1.12-.63 1.52M27.12.4l-2.65 2.67a1.34 1.34 0 0 0 0 1.9l10.4 10.4c.52.52 1.36.52 1.89 0l2.66-2.67c.52-.52.52-1.37 0-1.9L29.02.4a1.34 1.34 0 0 0-1.9 0M8.47 34.42a1.2 1.2 0 0 0 0-1.72l-1.35-1.36a1.2 1.2 0 0 0-1.72 0l-2.8 2.79-.75-.77a1.1 1.1 0 0 0-1.54 0 1.1 1.1 0 0 0 0 1.54l4.61 4.6a1.1 1.1 0 0 0 1.54 0 1.1 1.1 0 0 0 0-1.53l-.77-.76 2.8-2.8z" />
                <path fill="#425cc7" d="m22.56 8.03-5.92 5.92a1.37 1.37 0 0 0 0 1.91l3.65 3.65a6.6 6.6 0 0 1 8.53.7l2.95-2.95a1.37 1.37 0 0 0 0-1.92l-7.3-7.3a1.36 1.36 0 0 0-1.91 0m-3.8 13.03L16.6 18.9a1.3 1.3 0 0 0-1.84 0l-7.6 7.6a1.3 1.3 0 0 0 0 1.85l4.3 4.31a1.3 1.3 0 0 0 1.84 0l4.89-4.9a6.7 6.7 0 0 1 .57-6.7" />
              </svg>
            </div>
            <svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100" className="clickstack-svg-absolute clickstack-svg-right-center">
              <path fill="none" className="clickstack-line-bg" strokeWidth="3" d="M99 0V75A25 25 0 0 1 75 99H0" pathLength="1000" />
              <path fill="none" className="clickstack-line-animated" strokeLinecap="round" strokeWidth="3" d="M99 0V75A25 25 0 0 1 75 99H0" pathLength="1000">
                <animate attributeName="stroke-dasharray" dur="4s" keyTimes="0;0.5;1" repeatCount="indefinite" values="100 900;500 500;100 900" />
                <animate attributeName="stroke-dashoffset" calcMode="linear" dur="4s" keyTimes="0;0.5;1" repeatCount="indefinite" values="0;-1000;-2000" />
              </path>
            </svg>
          </div>

          {/* Middle Left - Empty */}
          <div />

          {/* Middle Center - ClickStack Logo */}
          <div className="clickstack-logo-container">
            <div className="clickstack-logo-glow" />
            <div className="clickstack-logo-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="179" height="179" fill="none" viewBox="0 0 179 179" className="clickstack-logo-svg">
                <path fill="#fff" d="M55 41.8c0-.43.37-.8.8-.8h6.16c.44 0 .81.37.81.8v68.4c0 .43-.37.8-.8.8H55.8a.8.8 0 0 1-.81-.8zm15.54 0c0-.43.37-.8.8-.8h6.16c.44 0 .8.37.8.8v68.4a.8.8 0 0 1-.8.8h-6.15a.8.8 0 0 1-.81-.8zm15.54 0c0-.43.36-.8.8-.8h6.16c.44 0 .8.37.8.8v68.4a.8.8 0 0 1-.8.8h-6.16a.8.8 0 0 1-.8-.8zm15.61 0c0-.43.37-.8.8-.8h6.16c.44 0 .81.37.81.8v68.4c0 .43-.37.8-.8.8h-6.16a.8.8 0 0 1-.8-.8zm15.54 27.27c0-.44.37-.8.8-.8h6.16c.44 0 .81.36.81.8V83a.8.8 0 0 1-.8.8h-6.16a.8.8 0 0 1-.81-.8zm-67.55 71.31q-1.06 0-1.9.38a4 4 0 0 0-1.44 1.08 5 5 0 0 0-.9 1.74q-.3 1.02-.3 2.3 0 1.69.5 2.92a4 4 0 0 0 1.5 1.88q.99.66 2.52.66.92 0 1.76-.16a18 18 0 0 0 1.74-.46v1.86q-.85.32-1.72.46-.89.16-2.04.16-2.19 0-3.64-.9a5.5 5.5 0 0 1-2.16-2.56 10 10 0 0 1-.72-3.88q0-1.62.44-2.96.45-1.36 1.32-2.34a6 6 0 0 1 2.12-1.5 8 8 0 0 1 2.94-.54 9.4 9.4 0 0 1 4 .88l-.8 1.8q-.7-.32-1.52-.56a6 6 0 0 0-1.7-.26M58.1 153H56v-15.2h2.12zm5.45-10.78V153h-2.12v-10.78zm-1.04-4.1q.48 0 .84.28t.36.94q0 .64-.36.94-.36.28-.84.28-.52 0-.88-.28-.34-.3-.34-.94 0-.66.34-.94.37-.28.88-.28m8.71 15.08q-1.47 0-2.6-.58a4 4 0 0 1-1.74-1.8 7 7 0 0 1-.62-3.14q0-2 .66-3.24a4.3 4.3 0 0 1 1.82-1.82 8 8 0 0 1 5.28-1.2q.9 0 1.68.2.81.18 1.32.42l-.64 1.72a10 10 0 0 0-1.2-.38 5 5 0 0 0-1.18-.16q-1 0-1.66.44-.63.41-.96 1.28-.3.84-.3 2.12 0 1.22.32 2.06.33.84.94 1.28.65.42 1.58.42a5.6 5.6 0 0 0 2.86-.72v1.84q-.57.35-1.26.5-.67.16-1.66.16m7.57-7.8q0 .42-.04.96l-.06 1h.06l.38-.48.48-.6.44-.52 3.3-3.54h2.46l-4.34 4.62L86.1 153h-2.5l-3.56-4.82-1.24 1.04V153h-2.1v-15.2h2.1zm17.5 3.74q0 1.28-.61 2.18t-1.78 1.4a7 7 0 0 1-2.76.48q-.78 0-1.5-.1a10 10 0 0 1-1.34-.24 6 6 0 0 1-1.1-.4v-2.04q.8.36 1.88.66 1.08.28 2.20.28.96 0 1.6-.26.64-.25.96-.72t.32-1.08q0-.66-.34-1.1-.32-.46-1.02-.84-.68-.4-1.84-.84-.8-.3-1.46-.66-.64-.38-1.14-.88a4 4 0 0 1-.74-1.16 4 4 0 0 1-.26-1.54q0-1.18.58-2.02.6-.84 1.64-1.28 1.05-.46 2.42-.46 1.16 0 2.14.24 1 .22 1.9.62l-.68 1.78a13 13 0 0 0-1.66-.56 7 7 0 0 0-1.76-.22q-.8 0-1.34.24t-.82.66q-.26.41-.26.98 0 .66.3 1.10t.96.82q.66.36 1.74.8 1.2.45 2.04 1t1.3 1.3.44 1.86m6.45 2.34a5 5 0 0 0 1.61-.26v1.6q-.33.16-.92.26-.57.12-1.16.12-.87 0-1.61-.3-.72-.3-1.16-1.04-.45-.75-.44-2.06v-5.96h-1.5v-.96l1.58-.8.74-2.28h1.3v2.42h3.08v1.62h-3.08v5.92q0 .88.42 1.3.44.42 1.14.42m7.95-9.46q2.04 0 3.04.9 1 .88 1 2.78v7.3h-1.5l-.42-1.5h-.08q-.45.58-.96.96-.48.38-1.12.56a5.5 5.5 0 0 1-3.26-.16q-.75-.36-1.2-1.08-.44-.74-.44-1.84 0-1.65 1.24-2.5 1.26-.86 3.82-.94l1.84-.06v-.6q0-1.2-.54-1.68a2.2 2.2 0 0 0-1.52-.48q-.84 0-1.6.24-.75.24-1.44.58l-.68-1.54a8 8 0 0 1 3.82-.94m.5 5.84q-1.84.08-2.56.62-.7.52-.7 1.48 0 .84.5 1.22.52.38 1.3.38 1.27 0 2.08-.7t.82-2.1v-.94zm11.15 5.34q-1.5 0-2.6-.58a4 4 0 0 1-1.74-1.8 7 7 0 0 1-.62-3.14q0-2 .66-3.24a4.3 4.3 0 0 1 1.82-1.82q1.16-.6 2.64-.6.9 0 1.68.2.8.18 1.32.42l-.64 1.72a10 10 0 0 0-1.2-.38q-.63-.16-1.18-.16-1 0-1.66.44-.65.41-.96 1.28-.3.84-.3 2.12 0 1.22.32 2.06.3.84.94 1.28.63.42 1.58.42a5.6 5.6 0 0 0 2.86-.72v1.84q-.6.35-1.26.5-.69.16-1.66.16m7.56-7.8q0 .42-.04.96-.01.55-.06 1h.06l.38-.48.48-.60.44-.52 3.3-3.54h2.46l-4.34 4.62 4.62 6.16h-2.5l-3.56-4.82-1.24 1.04V153h-2.1v-15.2h2.1z" />
              </svg>
            </div>
          </div>

          {/* Middle Right - Empty */}
          <div />

          {/* Bottom Left - Laptop with animation */}
          <div className="clickstack-grid-item">
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="44" height="44" fill="none" viewBox="0 0 44 44">
                <path stroke="#dfdfdf" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M33 9.17a3.67 3.67 0 0 1 3.67 3.66v15.63q0 .88.39 1.65L39 34a1.83 1.83 0 0 1-1.65 2.66H6.64A1.83 1.83 0 0 1 4.99 34l1.95-3.9q.4-.77.4-1.65V12.83A3.67 3.67 0 0 1 11 9.17zm3.77 20.14H7.23" />
              </svg>
            </div>
            <svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100" className="clickstack-svg-absolute clickstack-svg-bottom-left">
              <path fill="none" className="clickstack-line-bg" strokeWidth="3" d="M1 100V25A25 25 0 0 1 25 1H100" pathLength="1000" />
              <path fill="none" className="clickstack-line-animated" strokeLinecap="round" strokeWidth="3" d="M1 100V25A25 25 0 0 1 25 1H100" pathLength="1000">
                <animate attributeName="stroke-dasharray" dur="2.5s" keyTimes="0;0.5;1" repeatCount="indefinite" values="100 900;500 500;100 900" />
                <animate attributeName="stroke-dashoffset" calcMode="linear" dur="2.5s" keyTimes="0;0.5;1" repeatCount="indefinite" values="0;-1000;-2000" />
              </path>
            </svg>
          </div>

          {/* Bottom Center - Server with vertical line */}
          <div className="clickstack-grid-column">
            <svg xmlns="http://www.w3.org/2000/svg" width="3" height="115" viewBox="0 0 3 115">
              <line x1="0" x2="0" y1="115" y2="0" strokeWidth="3" className="clickstack-line-bg" />
              <line x1="0" x2="0" y1="115" y2="0" strokeWidth="3" className="clickstack-line-animated">
                <animate attributeName="y2" values="230;230;-230;172.5;-115;241.5;-126.5;115" keyTimes="0;0.148;0.407;0.407;0.704;0.704;1;1" dur="6.75s" repeatCount="indefinite" />
                <animate attributeName="y1" values="115;115;-460;115;-287.5;115;-241.5;115" keyTimes="0;0.148;0.407;0.407;0.704;0.704;1;1" dur="6.75s" repeatCount="indefinite" />
              </line>
            </svg>
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" fill="none" viewBox="0 0 40 40">
                <path fill="#f5a800" d="M21.05 21.77a4.35 4.35 0 1 0 6.03 6.27 4.35 4.35 0 0 0-6.03-6.27m4.6 4.6a2.16 2.16 0 0 1-3.67-1.52 2.16 2.16 0 0 1 4.3 0c0 .57-.23 1.12-.63 1.52M27.12.4l-2.65 2.67a1.34 1.34 0 0 0 0 1.9l10.4 10.4c.52.52 1.36.52 1.89 0l2.66-2.67c.52-.52.52-1.37 0-1.9L29.02.4a1.34 1.34 0 0 0-1.9 0M8.47 34.42a1.2 1.2 0 0 0 0-1.72l-1.35-1.36a1.2 1.2 0 0 0-1.72 0l-2.8 2.79-.75-.77a1.1 1.1 0 0 0-1.54 0 1.1 1.1 0 0 0 0 1.54l4.61 4.6a1.1 1.1 0 0 0 1.54 0 1.1 1.1 0 0 0 0-1.53l-.77-.76 2.8-2.8z" />
                <path fill="#425cc7" d="m22.56 8.03-5.92 5.92a1.37 1.37 0 0 0 0 1.91l3.65 3.65a6.6 6.6 0 0 1 8.53.7l2.95-2.95a1.37 1.37 0 0 0 0-1.92l-7.3-7.3a1.36 1.36 0 0 0-1.91 0m-3.8 13.03L16.6 18.9a1.3 1.3 0 0 0-1.84 0l-7.6 7.6a1.3 1.3 0 0 0 0 1.85l4.3 4.31a1.3 1.3 0 0 0 1.84 0l4.89-4.9a6.7 6.7 0 0 1 .57-6.7" />
              </svg>
            </div>
          </div>

          {/* Bottom Right - Game controller with animation */}
          <div className="clickstack-grid-item">
            <div className="clickstack-icon-box">
              <svg xmlns="http://www.w3.org/2000/svg" width="44" height="44" fill="none" viewBox="0 0 44 44">
                <path stroke="#dfdfdf" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M11 20.17h7.33m-3.66-3.67v7.33M27.5 22h.02M33 18.33h.02m-1.27-9.16h-19.5a7.33 7.33 0 0 0-7.3 6.58q0 .14-.03.28c-.15 1.23-1.25 10.47-1.25 13.3a5.5 5.5 0 0 0 5.5 5.5c1.83 0 2.75-.91 3.66-1.83l2.6-2.6a3.7 3.7 0 0 1 2.59-1.07h7.96c.97 0 1.9.39 2.6 1.08L31.16 33c.91.92 1.83 1.83 3.66 1.83a5.5 5.5 0 0 0 5.5-5.5c0-2.83-1.1-12.07-1.25-13.3l-.03-.28a7.33 7.33 0 0 0-7.3-6.58" />
              </svg>
            </div>
            <svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100" className="clickstack-svg-absolute clickstack-svg-bottom-right">
              <path fill="none" className="clickstack-line-bg" strokeWidth="3" d="M99 100V25A25 25 0 0 0 75 1H0" pathLength="1000" />
              <path fill="none" className="clickstack-line-animated" strokeLinecap="round" strokeWidth="3" d="M99 100V25A25 25 0 0 0 75 1H0" pathLength="1000">
                <animate attributeName="stroke-dasharray" dur="3.5s" keyTimes="0;0.5;1" repeatCount="indefinite" values="100 900;500 500;100 900" />
                <animate attributeName="stroke-dashoffset" calcMode="linear" dur="3.5s" keyTimes="0;0.5;1" repeatCount="indefinite" values="0;-1000;-2000" />
              </path>
            </svg>
          </div>
        </div>
      </div>
    </div>
  );
};